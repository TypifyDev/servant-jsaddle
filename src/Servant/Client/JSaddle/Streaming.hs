{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
-- | Streaming support for the JSaddle XHR client.
--
-- Three things in this module:
--
--   * 'sendStreamingRequest' — JSaddle-native primitive. Fires an XHR
--     and invokes a caller-supplied callback as bytes arrive
--     (@progress@), the request completes (@load@), or it fails
--     (@error@). No FRP types — bridge to your event system of choice
--     using this as the bottom layer.
--
--   * 'CaptureRequest' / 'getRequest' — a tiny monad whose 'RunClient'
--     and 'RunStreamingClient' instances *capture* the 'Request'
--     instead of executing it. Lets callers extract a type-derived
--     'Request' from any servant typed-client call and then hand it to
--     'sendStreamingRequest' directly (i.e. type-driven URL/headers
--     without paying for ClientM's actual execution).
--
--   * 'RunStreamingClient' instance for 'ClientM' — degraded: waits for
--     the full response then yields it as a single chunk through
--     'SourceT'. This makes servant APIs containing 'Stream' / 'StreamGet'
--     compile against 'ClientM'; for progressive-as-it-arrives semantics
--     reach for 'sendStreamingRequest' directly.
module Servant.Client.JSaddle.Streaming
  ( -- * JSaddle-native streaming
    StreamEvent (..)
  , sendStreamingRequest
    -- * Type-driven Request capture
  , CaptureRequest
  , runCaptureRequest
  , getRequest
  , captureRequest
  ) where

import           Control.Concurrent.MVar             (newEmptyMVar, takeMVar,
                                                       tryPutMVar)
import           Control.Concurrent.STM              (atomically)
import           Control.Concurrent.STM.TBQueue      (newTBQueueIO,
                                                       readTBQueue, writeTBQueue)
import           Control.Monad                       (void, when)
import           Control.Monad.IO.Class              (liftIO)
import           Control.Monad.Reader                (ask)
import qualified Data.ByteString                     as BS
import           Data.IORef                          (newIORef, readIORef,
                                                       writeIORef)
import           Data.Text                           (Text)
import qualified Data.Text                           as T
import qualified Data.Text.Encoding                  as TE

import qualified GHCJS.DOM.EventM                    as JSDOM
import qualified GHCJS.DOM.Types                     as JS
import qualified GHCJS.DOM.XMLHttpRequest            as JS
import qualified GHCJS.DOM.XMLHttpRequestEventTarget as JSXRET

import           Network.HTTP.Types                  (http11, mkStatus)

import           Control.Monad.Error.Class           (MonadError (..))
import           Data.Proxy                          (Proxy (..))
import           Servant.Client.Core
                 ( Client, ClientError, HasClient, Request, ResponseF (..),
                   RunClient (..), RunStreamingClient (..),
                   clientIn, requestMethod )
import qualified Servant.Types.SourceT               as ST

import           Servant.Client.Internal.JSaddleXhrClient
                 ( ClientEnv (..), ClientM (..), decodeUtf8Lenient, sendXhr,
                   setHeaders, toBody, toUrl )

-- ────────────────────────────────────────────────────────────────────
-- Stream events (Layer 1: jsaddle-native, no FRP)
-- ────────────────────────────────────────────────────────────────────

-- | One event in the lifetime of a streaming XHR.
data StreamEvent
  = StreamChunk !BS.ByteString
    -- ^ Bytes received since the previous event.
  | StreamDone  !Word
    -- ^ The XHR's @load@ event fired. Payload is the final HTTP status.
    -- No further events follow.
  | StreamFail  !Text
    -- ^ The XHR's @error@ event fired (network error, abort, …).
    -- No further events follow.
  deriving (Show, Eq)

-- | Fire an XHR built from a servant 'Request'. Each event arriving
-- from the transport (chunk, completion, error) invokes the supplied
-- callback exactly once, in arrival order. The callback runs in
-- 'JS.DOM', so any 'JSaddle' / IO action is available there.
--
-- The XHR uses the default response type so that
-- 'XMLHttpRequest.responseText' is readable as bytes arrive — chunk
-- positions are tracked across @progress@ firings.
sendStreamingRequest
  :: ClientEnv
  -> Request
  -> (StreamEvent -> JS.DOM ())
  -> JS.DOM ()
sendStreamingRequest env request fire = do
    let burl  = baseUrl env
        fixUp = fixUpXhr env
    xhr <- JS.newXMLHttpRequest

    let username, password :: Maybe JS.JSString
        username = Nothing; password = Nothing
    JS.open xhr (decodeUtf8Lenient $ requestMethod request) (toUrl burl request) True username password
    setHeaders xhr request
    fixUp xhr

    posRef <- liftIO (newIORef 0)

    let pushNew = do
          mtxt <- JS.getResponseText xhr
          case mtxt of
            Nothing -> pure ()
            Just s  -> do
              let txt   = T.pack s
                  total = T.length txt
              pos <- liftIO (readIORef posRef)
              when (total > pos) $ do
                let chunk = TE.encodeUtf8 (T.drop pos txt)
                liftIO (writeIORef posRef total)
                fire (StreamChunk chunk)

    void $ JSDOM.on xhr JSXRET.progress (JS.liftJSM pushNew)
    void $ JSDOM.on xhr JSXRET.load $ JS.liftJSM $ do
      pushNew
      s <- JS.getStatus xhr
      fire (StreamDone (fromIntegral s))
    void $ JSDOM.on xhr JSXRET.error $ JS.liftJSM $
      fire (StreamFail "network error")

    sendXhr xhr (toBody request)

-- ────────────────────────────────────────────────────────────────────
-- RunStreamingClient (Layer 2: degraded — full-then-yield)
-- ────────────────────────────────────────────────────────────────────

-- | Progressive 'RunStreamingClient': bridges the async XHR push model
-- into the 'SourceIO' sync-pull model used by 'withStreamingRequest'.
--
-- A bounded 'TBQueue' buffers chunks between the producer (XHR
-- callback firing per @progress@ event) and the consumer (the @k@
-- callback's 'SourceT' iteration). An 'MVar' lets the consumer block
-- until the first event arrives so we know the HTTP status before
-- constructing the 'Response'.
--
-- Trade-offs: the response status is inferred (200 on first chunk,
-- exact on @load@) and 'responseHeaders' is empty. For
-- header-sensitive callers use 'sendStreamingRequest' directly.
instance RunStreamingClient ClientM where
  withStreamingRequest req k = do
    env  <- ask
    domc <- ClientM JS.askDOM
    liftIO $ do
      queue    <- newTBQueueIO 64
      respMVar <- newEmptyMVar
      flip JS.runDOM domc $ sendStreamingRequest env req $ \case
        StreamChunk c -> liftIO $ do
          _ <- tryPutMVar respMVar (mkStatus 200 "")
          atomically (writeTBQueue queue (Just c))
        StreamDone s  -> liftIO $ do
          _ <- tryPutMVar respMVar (mkStatus (fromIntegral s) "")
          atomically (writeTBQueue queue Nothing)
        StreamFail _  -> liftIO $
          atomically (writeTBQueue queue Nothing)
      status <- takeMVar respMVar
      let pull = ST.Effect $ atomically (readTBQueue queue) >>= \case
            Just c  -> pure (ST.Yield c pull)
            Nothing -> pure ST.Stop
          resp = Response status mempty http11 (ST.SourceT ($ pull))
      k resp

-- ────────────────────────────────────────────────────────────────────
-- CaptureRequest: extract a type-derived Request without executing
-- ────────────────────────────────────────────────────────────────────

-- | A monad whose 'RunClient' / 'RunStreamingClient' instances capture
-- the 'Request' built by servant's typed client without actually
-- issuing it. Run a typed client call inside this monad, then pull the
-- 'Request' out via 'getRequest'.
--
-- Use case: wire a typed servant client API through to a custom
-- transport (e.g. 'sendStreamingRequest') that takes a 'Request'
-- directly, while keeping URL/headers/query construction type-driven.
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
-- Returns 'Nothing' if the action returned via 'pure' without ever
-- calling 'runRequest' / 'withStreamingRequest' (which only happens for
-- degenerate / empty client actions).
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
