{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
-- | Streaming support for the JSaddle client.
--
-- The 'RunStreamingClient' instance for the 'ClientM' from
-- "Servant.Client.Internal.JSaddleXhrClient" is implemented here. It
-- is backed by the browser @fetch@ API and @ReadableStream@: each
-- 'BodyReader'-style pull from the servant-side 'ST.SourceT'
-- corresponds to one @reader.read()@ on the response stream, so
-- chunks are forwarded to the consumer as they arrive on the wire.
-- The fetch runs to natural completion; abandoning the 'ST.SourceT'
-- mid-stream leaks the response body until the browser closes it.
--
-- Also exports 'CaptureRequest': a tiny monad whose 'RunClient' and
-- 'RunStreamingClient' instances *capture* the 'Request' that
-- servant's typed client constructs instead of executing it. Useful
-- for piping a typed servant API definition through to a custom
-- transport (e.g. websockets, server-sent events) that takes a
-- 'Request' directly while keeping URL / headers / query construction
-- type-driven.
module Servant.Client.JSaddle.Streaming
  ( -- * Type-driven Request capture
    CaptureRequest
  , runCaptureRequest
  , getRequest
  , captureRequest
  ) where

import           Control.Concurrent.MVar     (newEmptyMVar, putMVar, takeMVar)
import           Control.Exception           (throwIO)
import           Control.Monad               (forM_, void)
import           Control.Monad.Error.Class   (MonadError (..))
import           Control.Monad.IO.Class      (liftIO)
import           Control.Monad.Reader        (ask)
import qualified Data.ByteString             as BS
import qualified Data.ByteString.Lazy        as BSL
import           Data.CaseInsensitive        (mk, original)
import           Data.Foldable               (toList)
import           Data.IORef                  (modifyIORef', newIORef, readIORef)
import           Data.Proxy                  (Proxy (..))
import qualified Data.Sequence               as Seq
import qualified Data.Text                   as T
import qualified Data.Text.Encoding          as TE

import qualified GHCJS.Buffer                as Buffer
import qualified GHCJS.DOM.Types             as JS
import           GHCJS.Marshal.Pure          (pFromJSVal)
import qualified JavaScript.TypedArray.ArrayBuffer as ArrayBuffer

import           Lens.Micro                  ((^.))
import           Language.Javascript.JSaddle
                  ( JSM, JSVal, (#), freeFunction, fromJSValUnchecked,
                    function, js, js2, jsg, jss, new, obj, toJSVal,
                    valIsNull )
import qualified Language.Javascript.JSaddle as JSaddle

import           Network.HTTP.Media          (renderHeader)
import           Network.HTTP.Types          (Header, http11, mkStatus)

import qualified Servant.Types.SourceT       as ST
import           Servant.Client.Core

import           Servant.Client.Internal.JSaddleXhrClient
                  (ClientEnv (..), ClientM (..), decodeUtf8Lenient, toUrl)

-- ────────────────────────────────────────────────────────────────────
-- RunStreamingClient: fetch + ReadableStream
-- ────────────────────────────────────────────────────────────────────

-- | Each call to the SourceT's pull action corresponds to one
-- @reader.read()@ on the response's 'ReadableStream'. Bytes are
-- forwarded to the consumer as they arrive, not buffered until the
-- response completes. Status and headers are read from the @Response@
-- object before the body is touched, so they're exact (not inferred
-- from the first chunk like the previous XHR implementation).
instance RunStreamingClient ClientM where
  withStreamingRequest req k = do
    env <- ask
    ctx <- ClientM JSaddle.askJSM
    liftIO (JSaddle.runJSM (fetchStreaming env req k) ctx)

-- | Fire @req@ via the browser @fetch@ API and hand a 'Response' to
-- @k@, whose body is a 'ST.SourceT' driven by the response stream's
-- reader.
fetchStreaming
  :: ClientEnv
  -> Request
  -> (ResponseF (ST.SourceT IO BS.ByteString) -> IO a)
  -> JSM a
fetchStreaming env req k = do
  let url = toUrl (baseUrl env) req

  -- ── Build the fetch init object ──
  initObj <- obj

  do  mVal <- toJSVal (decodeUtf8Lenient (requestMethod req))
      initObj ^. jss ("method" :: T.Text) mVal

  hdrsObj <- new (jsg ("Headers" :: T.Text)) ()
  forM_ (toList (requestAccept req)) $ \media ->
    void $ hdrsObj # ("append" :: T.Text) $
      ( "Accept" :: T.Text
      , decodeUtf8Lenient (renderHeader media) )
  case requestBody req of
    Just (_, media) ->
      void $ hdrsObj # ("append" :: T.Text) $
        ( "Content-Type" :: T.Text
        , decodeUtf8Lenient (renderHeader media) )
    Nothing -> pure ()
  forM_ (toList (requestHeaders req)) $ \(name, value) ->
    void $ hdrsObj # ("append" :: T.Text) $
      ( decodeUtf8Lenient (original name)
      , decodeUtf8Lenient value )
  do  hVal <- toJSVal hdrsObj
      initObj ^. jss ("headers" :: T.Text) hVal

  case requestBody req of
    Nothing                       -> pure ()
    Just (RequestBodyBS bs, _)    -> do
      b <- bsToArrayBuffer bs
      initObj ^. jss ("body" :: T.Text) b
    Just (RequestBodyLBS lbs, _)  -> do
      b <- bsToArrayBuffer (BSL.toStrict lbs)
      initObj ^. jss ("body" :: T.Text) b
    Just (RequestBodySource _, _) ->
      liftIO (throwIO (userError "RequestBodySource isn't supported"))

  -- ── Fire the fetch, await the Response ──
  global  <- jsg ("globalThis" :: T.Text)
  promise <- global ^. js2 ("fetch" :: T.Text) url initObj
  resp    <- awaitJSPromise promise >>= \case
    Right r -> pure r
    Left  _ -> liftIO (throwIO (userError "fetch failed"))

  statusN     <- fromJSValUnchecked =<< resp ^. js ("status"     :: T.Text)
  statusTextS <- fromJSValUnchecked =<< resp ^. js ("statusText" :: T.Text)
  hdrs        <- readResponseHeaders resp

  -- ── Build the SourceT body backed by the reader ──
  bodyVal  <- resp ^. js ("body" :: T.Text)
  bodyNull <- valIsNull bodyVal
  ctx      <- JSaddle.askJSM

  source <-
    if bodyNull
      then pure (ST.SourceT ($ ST.Stop))
      else do
        reader <- bodyVal # ("getReader" :: T.Text) $ ()
        let pullIO :: IO BS.ByteString
            pullIO = JSaddle.runJSM (pullChunk reader) ctx
            step = ST.Effect $ do
              bs <- pullIO
              pure $ if BS.null bs then ST.Stop else ST.Yield bs step
        pure (ST.SourceT ($ step))

  let resp' = Response
        { responseStatusCode  =
            mkStatus statusN (TE.encodeUtf8 (T.pack statusTextS))
        , responseHeaders     = Seq.fromList hdrs
        , responseHttpVersion = http11
        , responseBody        = source
        }

  -- NB: we intentionally do *not* bracket the fetch with an abort on
  -- `k`'s return. servant-client-core's typed Stream client calls k
  -- with the raw response, wraps the body lazily with framing /
  -- decoding, and returns the wrapped SourceT — consumed *after* k
  -- (and therefore after withStreamingRequest) returns. Aborting on
  -- k's return would tear the fetch down before any chunk is pulled.
  -- The fetch runs to natural completion; a SourceT that is abandoned
  -- mid-stream will leak the response body until the browser closes
  -- it (TODO: wire an AbortController-based cleanup hook through to
  -- the source's drain path).
  liftIO (k resp')

-- ────────────────────────────────────────────────────────────────────
-- JSM helpers: promise await, chunk conversion, header iteration
-- ────────────────────────────────────────────────────────────────────

-- | Block the current JSM thread until @p@ settles. Right = resolved
-- value, Left = rejection reason.
--
-- Works under both jsaddle-warp (the JS-side @.then@ callback round-
-- trips back to the Haskell-side function over the bridge and unblocks
-- the 'MVar') and GHCJS in-browser (the green thread suspends on
-- 'takeMVar' and resumes when the callback runs).
awaitJSPromise :: JSVal -> JSM (Either JSVal JSVal)
awaitJSPromise p = do
  mv <- liftIO newEmptyMVar
  cbResolve <- function $ \_ _ args -> do
    v <- case args of (x:_) -> pure x; _ -> toJSVal ()
    liftIO (putMVar mv (Right v))
  cbReject <- function $ \_ _ args -> do
    v <- case args of (x:_) -> pure x; _ -> toJSVal ()
    liftIO (putMVar mv (Left v))
  _ <- p ^. js2 ("then" :: T.Text) cbResolve cbReject
  r <- liftIO (takeMVar mv)
  freeFunction cbResolve
  freeFunction cbReject
  pure r

-- | One read from a @ReadableStreamDefaultReader@. Returns the chunk
-- as a 'BS.ByteString'; empty 'BS.ByteString' signals EOF (matching
-- the 'BodyReader' convention from @http-client@). Empty chunks from
-- the underlying stream are looped over so the EOF signal stays
-- unambiguous, and read errors are mapped to EOF.
pullChunk :: JSVal -> JSM BS.ByteString
pullChunk reader = do
  promise <- reader # ("read" :: T.Text) $ ()
  awaitJSPromise promise >>= \case
    Left _ -> pure BS.empty
    Right chunkObj -> do
      done <- fromJSValUnchecked =<< chunkObj ^. js ("done" :: T.Text)
      if done
        then pure BS.empty
        else do
          valV <- chunkObj ^. js ("value" :: T.Text)
          bs   <- u8ArrayToBS valV
          if BS.null bs then pullChunk reader else pure bs

-- | Copy a 'Uint8Array' (or any typed-array view) into a strict
-- 'BS.ByteString' via 'GHCJS.Buffer'. Reads @.buffer@, @.byteOffset@,
-- @.byteLength@ from the view so views over a larger backing buffer
-- are sliced correctly.
u8ArrayToBS :: JSVal -> JSM BS.ByteString
u8ArrayToBS u8 = do
  abVal   <- u8 ^. js ("buffer"     :: T.Text)
  offset  <- fromJSValUnchecked =<< u8 ^. js ("byteOffset" :: T.Text)
  byteLen <- fromJSValUnchecked =<< u8 ^. js ("byteLength" :: T.Text)
  let mab = pFromJSVal abVal :: ArrayBuffer.MutableArrayBuffer
  ab  <- liftIO (ArrayBuffer.unsafeFreeze mab)
  buf <- JSaddle.ghcjsPure (Buffer.createFromArrayBuffer ab)
  JSaddle.ghcjsPure (Buffer.toByteString offset (Just byteLen) buf)

-- | Wrap a strict 'BS.ByteString' as a fresh 'ArrayBuffer' suitable
-- for use as @fetch@'s @body@ init field. Copies, so the source
-- bytestring's pinned buffer can be freed independently.
bsToArrayBuffer :: BS.ByteString -> JSM JSVal
bsToArrayBuffer bs = do
  (b, _off, _len) <- JSaddle.ghcjsPure (Buffer.fromByteString (BS.copy bs))
  mb <- Buffer.thaw b
  ab <- JSaddle.ghcjsPure (Buffer.getArrayBuffer mb)
  pure (JS.pToJSVal ab)

-- | Iterate the @Headers@ object via its @forEach@ method. The
-- callback receives @(value, key, parent)@; we collect into a list,
-- reversed at the end so order matches arrival.
readResponseHeaders :: JSVal -> JSM [Header]
readResponseHeaders respVal = do
  hdrsVal <- respVal ^. js ("headers" :: T.Text)
  ref     <- liftIO (newIORef [])
  cb <- function $ \_ _ args -> case args of
    (v:k:_) -> do
      kT <- fromJSValUnchecked k :: JSM T.Text
      vT <- fromJSValUnchecked v :: JSM T.Text
      liftIO $ modifyIORef' ref
        ((mk (TE.encodeUtf8 kT), TE.encodeUtf8 vT) :)
    _ -> pure ()
  cbVal <- toJSVal cb
  _ <- hdrsVal # ("forEach" :: T.Text) $ cbVal
  freeFunction cb
  reverse <$> liftIO (readIORef ref)

-- ────────────────────────────────────────────────────────────────────
-- CaptureRequest: extract a type-derived Request without executing
-- ────────────────────────────────────────────────────────────────────

-- | A monad whose 'RunClient' / 'RunStreamingClient' instances capture
-- the 'Request' built by servant's typed client without actually
-- issuing it. Run a typed client call inside this monad, then pull the
-- 'Request' out via 'getRequest'.
--
-- Use case: wire a typed servant client API through to a custom
-- transport that takes a 'Request' directly, while keeping URL /
-- headers / query construction type-driven.
newtype CaptureRequest a = CaptureRequest
  { runCaptureRequest :: Either Request a }
  deriving (Functor)

instance Applicative CaptureRequest where
  pure = CaptureRequest . Right
  CaptureRequest (Left r) <*> _ = CaptureRequest (Left r)
  _ <*> CaptureRequest (Left r) = CaptureRequest (Left r)
  CaptureRequest (Right f) <*> CaptureRequest (Right x) =
    CaptureRequest (Right (f x))

instance Monad CaptureRequest where
  CaptureRequest (Left r)  >>= _ = CaptureRequest (Left r)
  CaptureRequest (Right x) >>= f = f x

instance MonadError ClientError CaptureRequest where
  throwError _ = errorWithoutStackTrace
    "Servant.Client.JSaddle.Streaming.CaptureRequest: throwError unsupported"
  catchError m _ = m

instance RunClient CaptureRequest where
  runRequestAcceptStatus _ req = CaptureRequest (Left req)
  throwClientError = throwError

instance RunStreamingClient CaptureRequest where
  withStreamingRequest req _ = CaptureRequest (Left req)

-- | Pull the captured 'Request' out of a 'CaptureRequest' action.
-- 'Nothing' iff the action returned via 'pure' without ever calling
-- 'runRequest' / 'withStreamingRequest' (degenerate / empty clients).
getRequest :: CaptureRequest a -> Maybe Request
getRequest (CaptureRequest (Left r))  = Just r
getRequest (CaptureRequest (Right _)) = Nothing

-- | Build a typed client function whose monad is 'CaptureRequest', so
-- applying it with args yields a 'CaptureRequest' value from which the
-- 'Request' can be extracted via 'getRequest'.
captureRequest
  :: HasClient CaptureRequest api
  => Proxy api -> Client CaptureRequest api
captureRequest p = clientIn p (Proxy :: Proxy CaptureRequest)
