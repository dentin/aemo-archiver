{-# LANGUAGE TupleSections     #-}

module AEMO.WebScraper where

import           Network.HTTP                 (Request, Response, HStream, getRequest, rspBody,
                                               getAuth, failHTTPS, openStream, host, port,
                                               normalizeRequest, defaultNormalizeRequestOptions,
                                               rqBody, rqURI, normDoClose, simpleHTTP_)
import           Network.Stream               (ConnError (..), Result)
import           Network.URI                  (parseURI, parseURIReference, relativeTo, escapeURIString)
import           Text.HTML.TagSoup            (Tag (TagOpen), parseTags)
import           Data.Char                    (toLower)
import           Data.Maybe                   (fromMaybe, mapMaybe)
import           Data.List                    (isSuffixOf, isInfixOf)
import           Data.ByteString.Lazy         (ByteString)
import qualified Data.ByteString.Lazy  as BSL (empty)
import           Control.Exception            (SomeException, catch)
import           Codec.Archive.Zip            (toArchive, filesInArchive, findEntryByPath, fromEntry)
import           Control.DeepSeq              (NFData, deepseq)
import           Data.List.Split              (splitOn)
import           AEMO.Types                   (FileName)

type URL = String


aemo5mPSURL :: URL
aemo5mPSURL =  "http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/"

aemoPSArchiveURL :: URL
aemoPSArchiveURL =  "http://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/"


-- | Given a URL, finds all HTML links on the page
getARefs :: URL -> IO [String]
getARefs url = do
    ersp <- simpleHTTPSafe (getRequest url)
    case ersp of
        Left err -> print err >> return []
        Right rsp -> do
            let tags = parseTags (rspBody rsp)
            return [val | (TagOpen n attrs) <- tags, n `elem` ["a","A"],
                          (key,val) <- attrs, key `elem` ["href","HREF"]
                          ]


-- | Takes a URL and finds all zip files linked from it.
joinLinks :: URL -> IO [(FileName, URL)]
joinLinks url = do
    links <- getARefs url
    return . mapMaybe (joinURIs url) . filter (isSuffixOf ".zip" . map toLower) $ links


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
fetchFiles :: [(FileName, URL)] -> IO [(FileName, Either String ByteString)]
fetchFiles urls = mapM fetch urls


-- | Fetch an individual file.
fetch :: (FileName, URL) -> IO (FileName, Either String ByteString)
fetch (fn, url) = do
    res <- simpleHTTPSafe ((getRequest url) {rqBody = BSL.empty})
            `catch` (\e -> return $ Left (ErrorMisc (show (e :: SomeException))))
    putChar '.'
    return $! (fn,) $! case res of
        Right bs -> Right . rspBody $! bs
        Left err -> Left . show $! err


-- | Takes URLs and zip files and extracts all files with a particular suffix from each zip file
extractFiles :: String -> [(URL, ByteString)] -> [Either (URL, String) (FileName, ByteString)]
extractFiles suf arcs = concatMap extract arcs where
    extract (url,bs) =
        let arc = toArchive bs
            paths = filesInArchive arc
            files = filter (isSuffixOf suf . map toLower) paths
        in case files of
            []  -> [Left (url, "No " ++ suf ++ " found in " ++ url)]
            fs  -> map ext fs where
                    ext f = case findEntryByPath f arc of
                        Nothing -> Left  (url, concat ["Could not find ", f, " in archive ", url])
                        Just e  -> Right (f, fromEntry e)


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

