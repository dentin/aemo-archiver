{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}


module AEMO.WebScraper where

import           AEMO.Types           (FileName)

import           Codec.Archive.Zip    (filesInArchive, findEntryByPath,
                                       fromEntry, toArchiveOrFail)

import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy.Char8 as C8

import           Data.Char            (toLower)
import           Data.List            (isSuffixOf)
import           Data.List.Split      (splitOn)
import           Data.Maybe           (mapMaybe)

import           Control.Lens
import           Network.Wreq

import           Network.URI          (escapeURIString, parseURI,
                                       parseURIReference, relativeTo)

import           Text.HTML.TagSoup    (Tag (TagOpen), parseTags)
import           Text.StringLike      (castString)

type URL = String


aemo5mPSURL :: URL
aemo5mPSURL =  "http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/"

aemoPSArchiveURL :: URL
aemoPSArchiveURL =  "http://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/"



-- | Given a URL, finds all HTML links on the page
getARefs :: Options -> URL -> IO [String]
getARefs opts url = do
    -- ersp <- simpleHTTPSafe (getRequest url)
    rsp <- getWith opts url :: IO (Response ByteString)
    let tags = parseTags (rsp ^. responseBody)
    return [castString val
           | (TagOpen n attrs) <- tags, C8.map toLower n == "a"
           , (key,val) <- attrs,        C8.map toLower key == "href"
           ]


-- | Takes a URL and finds all zip files linked from it.
joinLinks :: Options -> URL -> IO [(FileName, URL)]
joinLinks opts url = do
    links <- getARefs opts url
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
fetchFiles :: Options -> [(FileName, URL)] -> IO [(FileName, Either String ByteString)]
fetchFiles opts urls = mapM (fetch opts) urls


-- | Fetch an individual file.
fetch :: Options -> (FileName, URL) -> IO (FileName, Either String ByteString)
fetch conf (fn, url) = do
    res <- getWith conf url
    putChar '.'
    return $! (fn,) . Right $! res ^. responseBody


-- | Takes URLs and zip files and extracts all files with a particular suffix from each zip file
extractFiles :: String -> [(URL, ByteString)] -> [Either (URL, String) (FileName, ByteString)]
extractFiles suf arcs = concatMap extract arcs where
    extract (url,bs) =
        let arc = toArchiveOrFail bs
            paths = fmap filesInArchive arc
            files = fmap (filter (isSuffixOf suf . map toLower)) paths
        in case files of
            Left err -> [Left (url, "Error parsing zip file: " ++ err)]
            Right f -> case f of
                []  -> [Left (url, "No " ++ suf ++ " found in " ++ url)]
                fs  -> map ext fs where
                        ext file = case findEntryByPath file ((\(Right x) -> x) arc) of
                            Nothing -> Left  (url, concat ["Could not find ", file, " in archive ", url])
                            Just e  -> Right (file, fromEntry e)

filename :: URL -> String
filename url = last (splitOn "/" url)
