{-# LANGUAGE TupleSections #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TemplateHaskell #-}

module AEMO.WebScraper
    ( aemo5mPSURL
    , aemoPSArchiveURL
    , getARefs
    , joinLinks
    , fetchFiles
    , extractFiles
    ) where

import           AEMO.Types           (FileName,ZipName(..))
import           Codec.Archive.Zip    (filesInArchive, findEntryByPath,
                                       fromEntry, toArchiveOrFail)
import           Control.DeepSeq      (NFData, deepseq)
import           Control.Exception    (SomeException, catch)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BSL (empty)
import           Data.Char            (toLower)
import           Data.List            (isSuffixOf)
import           Data.List.Split      (splitOn)
import           Data.Maybe           (fromMaybe)
import           Network.HTTP         (HStream, Request, Response,
                                       defaultNormalizeRequestOptions,
                                       failHTTPS, getAuth, getRequest, host,
                                       normDoClose, normalizeRequest,
                                       openStream, port, rqBody, rqURI, rspBody,
                                       simpleHTTP_)
import           Network.Stream       (ConnError (..), Result)
import           Network.URI          (escapeURIString, parseURI,
                                       parseURIReference, relativeTo)
import           Text.HTML.TagSoup    (Tag (TagOpen), parseTags)

import           Data.Conduit (Conduit, Producer)
import qualified Data.Conduit as C

import           Data.Conduit.List (sourceList)
import qualified Data.Conduit.List as CL

import Data.Functor

import Data.Char (toUpper)

import Control.Monad.IO.Class

import Control.Monad.Logger
import qualified Data.Text as T


type URL = String


aemo5mPSURL :: URL
aemo5mPSURL =  "http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/"

aemoPSArchiveURL :: URL
aemoPSArchiveURL =  "http://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/"


-- | Given a URL, finds all HTML links on the page
getARefs :: (MonadIO m) => URL -> Producer m String
getARefs url = do
    ersp <- liftIO $ simpleHTTPSafe (getRequest url)
    case ersp of
        Left err -> error (show err) -- TODO: Do this properly
        Right rsp -> do
            let tags = parseTags (rspBody rsp)
            sourceList [val | (TagOpen n attrs) <- tags , map toUpper n == "A",
                              (key,val)         <- attrs, map toUpper key == "HREF"
                            ]


-- | Takes a URL and finds all zip files linked from it.
-- joinLinks :: URL -> IO [(FileName, URL)]
joinLinks :: (MonadIO m) => URL -> Conduit String m (FileName,URL)
joinLinks url = CL.filter (isSuffixOf ".zip" . map toLower) C.=$= CL.mapMaybe (joinURIs url)


-- | Takes a base URL and a path relative to that URL and joins them:
--      joinURIs "http://example.com/foo/bar" "/baz/quux.txt" -> Just "http://example.com/baz/quux.txt"
--      joinURIs "http://example.com/foo/bar" "baz/quux.txt"  -> Just "http://example.com/foo/baz/quux.txt"
--      joinURIs "http://example.com/foo/bar/" "baz/quux.txt" -> Just "http://example.com/foo/bar/baz/quux.txt"
joinURIs :: URL -> String -> Maybe (FileName, URL)
joinURIs base relative = do
    buri <- parseURI          base
    ruri <- parseURIReference $ escapeURIString (/=' ') relative
    return (filename relative, show (ruri `relativeTo` buri))


-- | Given a list of URLs, attempts to fetch them all and pairs the result with
--   the url of the request.
-- fetchFiles :: [(FileName, URL)] -> IO [(FileName, Either String ByteString)]
fetchFiles :: (MonadIO m, MonadLogger m) => Conduit (ZipName, URL) m (FileName, ByteString)
fetchFiles = C.awaitForever $ \(ZipName z,url) -> do

    $(logInfo) $ T.pack ("fetchFiles: Fetching: " ++ url)
    etup <- liftIO $ fetch (z,url)
    case etup of
        (fn,Left err) -> $(logWarn) $ T.pack (concat ["fetchFiles: Failed to fetch ", fn, " from ", url,": ", err])
        (fn,Right bs) -> C.yield (fn,bs)


-- | Fetch an individual file.
fetch :: (FileName, URL) -> IO (FileName, Either String ByteString)
fetch (fn, url) = do
    res <- simpleHTTPSafe ((getRequest url) {rqBody = BSL.empty})
            `catch` (\e -> return $ Left (ErrorMisc (show (e :: SomeException))))
    return $! (fn,) $! case res of
        Right bs -> Right . rspBody $! bs
        Left err -> Left . show $! err


-- | Takes URLs and zip files and extracts all files with a particular suffix from each zip file
-- extractFiles :: String -> [(URL, ByteString)] -> [Either (URL, String) (FileName, ByteString)]
extractFiles :: (MonadIO m, MonadLogger m) => String -> Conduit (URL, ByteString) m ((ZipName,FileName), ByteString)
extractFiles suf = C.awaitForever $ \(url,bs) -> do
    let arc = toArchiveOrFail bs
        paths = fmap filesInArchive arc
        files = fmap (filter (isSuffixOf suf . map toLower)) paths
    case files of
        Left err -> $(logWarn) $ T.pack ("Error parsing zip file from " ++ url ++ ": " ++ err)
        Right f -> case f of
            []  -> $(logWarn) $ T.pack ("No " ++ suf ++ " found in " ++ url)
            fs  -> mapM_ ext fs where
                    ext fname = case findEntryByPath fname ((\(Right x) -> x) arc) of -- Partial safe here because code not reached
                                                                              -- if arc is Left
                        Nothing -> $(logWarn) $ T.pack (concat ["Could not find ", fname, " in archive ", url])
                        Just e  -> C.yield ((ZipName $ filename url,fname), fromEntry e)


simpleHTTPSafe :: (HStream ty, NFData ty) => Request ty -> IO (Result (Response ty))
simpleHTTPSafe r = do
    auth <- getAuth r
    failHTTPS (rqURI r)
    c <- openStream (host auth) (fromMaybe 80 (port auth))
    let norm_r = normalizeRequest defaultNormalizeRequestOptions{normDoClose=True} r
    res <- simpleHTTP_ c norm_r

    return $ case res of
        Left e -> Left e
        Right rsp -> rspBody rsp `deepseq` Right rsp


filename :: URL -> String
filename url = last (splitOn "/" url)

