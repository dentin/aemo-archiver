{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

module Main where

import Text.HTML.TagSoup
import Network.HTTP
-- import qualified Data.Text as T
-- import           Data.Text (Text)
-- import qualified Data.ByteString as BS
-- import           Data.ByteString (ByteString)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BSL

import           Data.Vector (Vector)
import qualified Data.Vector as V

import           Data.ByteString.Lazy.Char8 ()
import qualified Data.ByteString.Lazy.Char8 as C

import           Control.Concurrent.Async
import           Control.Exception          (SomeException, catch)
import           Data.Csv

import Network.URI

import Data.List (isSuffixOf)
import Data.Maybe (catMaybes)
import Data.Either (isLeft)


aemoURL :: String
aemoURL =  "http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/"


main = do
	zipLinks <- joinLinks aemoURL
	mapM_ print $ take 10 zipLinks
	putStrLn "..."
	fetched <- fetchFiles zipLinks
	mapM_ print (filter (isLeft . snd) fetched)
	putStr "Files fetched: "
	print (length fetched)


-- | Given a URL, finds all HTML links on the page
getARefs :: String -> IO [String]
getARefs url = do
	ersp <- simpleHTTP (getRequest url)
	case ersp of
		Left err -> print err >> return []
		Right rsp -> do
			let tags = parseTags (rspBody rsp)
			return [val | (TagOpen n attrs) <- tags, n `elem` ["a","A"],
						  (key,val) <- attrs, key `elem` ["href","HREF"]
						  ]

-- | Takes a URL and finds all zip files linked from it.
-- TODO: handle case insensitive matching of the .zip suffix
joinLinks :: String -> IO [String]
joinLinks url = do
	links <-getARefs url
	return . filter (isSuffixOf ".zip") . catMaybes .  map (joinURIs url) $ links


-- | Takes a base URL and a path relative to that URL and joins them:
-- 		joinURIs "http://example.com/foo/bar" "/baz/quux.txt" -> Just "http://example.com/baz/quux.txt"
-- 		joinURIs "http://example.com/foo/bar" "baz/quux.txt"  -> Just "http://example.com/foo/baz/quux.txt"
-- 		joinURIs "http://example.com/foo/bar/" "baz/quux.txt" -> Just "http://example.com/foo/bar/baz/quux.txt"
joinURIs :: String -> String -> Maybe String
joinURIs base relative = do
	buri <- parseURI         base
	ruri <- parseURIReference relative
	joined <- return $ ruri `relativeTo` buri
	return $ show joined

-- | Given a list of URLs, attempts to fetch them all and pairs the result with
--   the url of the request. It performs fetches concurrently in groups of 20
fetchFiles :: [String] -> IO [(String,Either String ByteString)]
fetchFiles urls =
	concat <$> mapM (mapConcurrently fetch) (chunksOf 20 urls) where
	-- mapM fetch urls where
	-- mapConcurrently fetch urls where
		fetch url = do
			res <- simpleHTTP ((getRequest url) {rqBody = BS.empty})
					`catch` (\e -> return$ Left (ErrorMisc (show (e :: SomeException))))
			-- if isLeft res
			-- 	then putStrLn $ "Failed to fetch: " ++ url
			-- 	else putStrLn $ "OK: " ++ url
			return $! (url,) $! case res of
				Right bs -> Right . rspBody $! bs
				Left err -> Left . show $! err




-- | (Will be) used to parse the AEMO CSV files which contain daily data
parseAEMO :: BSL.ByteString -> Either String (Vector (String, String, String, Int, String, String, Double))
parseAEMO file =
	-- Removes the beginning and end lines of the file
	-- AEMO data files have two headers and a footer which causes issues when parsing
	let trimmed = C.concat . init . drop 1 . C.lines $ file
	in decode HasHeader trimmed


