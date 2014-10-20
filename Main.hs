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

import Data.Csv
import Control.Concurrent.Async

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
	fetched <- fetchZipFiles zipLinks
	print fetched


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

joinLinks :: String -> IO [String]
joinLinks url = do
	links <-getARefs url
	return . filter (isSuffixOf ".zip") . catMaybes .  map (joinURIs url) $ links


joinURIs :: String -> String -> Maybe String
joinURIs base relative = do
	buri <- parseURI         base
	ruri <- parseURIReference relative
	joined <- return $ ruri `relativeTo` buri
	return $ show joined

fetchZipFiles :: [String] -> IO [(String,Either String ByteString)]
fetchZipFiles urls = mapM fetch urls where
	fetch url = do
		res <- simpleHTTP $ (getRequest url) {rqBody = BSL.empty}
		if isLeft res
			then putStrLn $ "Failed to fetch: " ++ url
			else putStrLn $ "OK: " ++ url
		return $ (url,) $ case res of
			Right bs -> Right $ rspBody bs
			Left err -> Left $ show err





parseAEMO :: ByteString -> Either String (Vector (String, String, String, Int, String, String, Double))
parseAEMO file =
	-- Removes the beginning and end lines of the file
	-- AEMO data files have two headers and a footer which causes issues when parsing
	let trimmed = C.concat . init . drop 1 . C.lines $ file
	in decode HasHeader trimmed


