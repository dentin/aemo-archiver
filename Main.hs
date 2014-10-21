{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import           Data.ByteString.Lazy       (ByteString)
import qualified Data.ByteString.Lazy       as BSL


import           Data.Vector                (Vector)
-- import qualified Data.Vector as V

import           Data.ByteString.Lazy.Char8 ()
import qualified Data.ByteString.Lazy.Char8 as C

import           Control.Concurrent.Async
import           Control.Exception          (SomeException, catch)
import           Data.Csv

import           Network.HTTP
import           Network.Stream             (ConnError (..), Result)
import           Network.URI
import           Text.HTML.TagSoup

import           Control.Applicative
import           Control.Arrow
import           Control.DeepSeq
import           Data.List                  (isSuffixOf)
import           Data.List.Split            (chunksOf)
import           Data.Maybe                 (fromMaybe, mapMaybe)

import           Codec.Archive.Zip


aemoURL :: String
aemoURL =  "http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/"


main :: IO ()
main = do
	zipLinks <- joinLinks aemoURL
	-- mapM_ print $ take 10 zipLinks
	-- putStrLn "..."
	fetched <- fetchFiles zipLinks
	let (ferrs,rslts) = partition' fetched
	if rslts `deepseq` null ferrs
		then return ()
		else putStrLn "Fetch failures:" >> mapM_ print ferrs
	putStr "Files fetched: "
	print (length rslts)
	let (eerrs,extracted) = partition' . extractCSVs $ rslts
	if extracted `deepseq` null eerrs
		then return ()
		else putStrLn "Extraction failures:" >> mapM_ print eerrs
	let (perrs, parsed) = partition' . map (second parseAEMO) $ extracted
	if extracted `deepseq` null perrs
		then return ()
		else putStrLn "Parsing failures:" >> mapM_ print perrs
	mapM_ print (take 1 parsed)
	return ()




-- | Given a URL, finds all HTML links on the page
getARefs :: String -> IO [String]
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
-- TODO: handle case insensitive matching of the .zip suffix
joinLinks :: String -> IO [String]
joinLinks url = do
	links <-getARefs url
	return . filter (isSuffixOf ".zip") . mapMaybe (joinURIs url) $ links


-- | Takes a base URL and a path relative to that URL and joins them:
-- 		joinURIs "http://example.com/foo/bar" "/baz/quux.txt" -> Just "http://example.com/baz/quux.txt"
-- 		joinURIs "http://example.com/foo/bar" "baz/quux.txt"  -> Just "http://example.com/foo/baz/quux.txt"
-- 		joinURIs "http://example.com/foo/bar/" "baz/quux.txt" -> Just "http://example.com/foo/bar/baz/quux.txt"
joinURIs :: String -> String -> Maybe String
joinURIs base relative = do
	buri <- parseURI         base
	ruri <- parseURIReference relative
	return $ show (ruri `relativeTo` buri)

-- | Given a list of URLs, attempts to fetch them all and pairs the result with
--   the url of the request. It performs fetches concurrently in groups of 20
fetchFiles :: [String] -> IO [(String,Either String ByteString)]
fetchFiles urls =
	concat <$> mapM (fmap force . mapConcurrently fetch) (chunksOf 40 urls) where
	-- mapM (fmap force . fetch) urls where
	-- mapConcurrently fetch urls where
		fetch url = do
			res <- simpleHTTPSafe ((getRequest url) {rqBody = BSL.empty})
					`catch` (\e -> return$ Left (ErrorMisc (show (e :: SomeException))))
			-- if isLeft res
			-- 	then putStrLn $ "Failed to fetch: " ++ url
			-- 	else putStrLn $ "OK: " ++ url
			return $! (url,) $! case res of
				Right bs -> Right . rspBody $! bs
				Left err -> Left . show $! err


extractCSVs :: [(String,ByteString)] -> [(String,Either String ByteString)]
extractCSVs arcs = map extract arcs where
	extract (name,bs) =
		let
			arc = toArchive bs
			paths = filesInArchive arc
			csvs = filter (".CSV" `isSuffixOf`) paths
		in case csvs of
			[] 		-> (name, Left $ "No CSVs found in " ++ name)
			(p:_) 	-> (name,) $ case findEntryByPath p arc of
				Nothing -> Left $ concat ["Could not find ", p, " in archive ", name]
				Just e -> Right $ fromEntry e


partition' :: [(a, Either b c)] -> ([(a,b)], [(a,c)])
partition' ps = go ps  where
	go [] = ([],[])
	go (x:xs) =
		let (ls,rs) = go xs
		in case x of
			(a, Left b ) -> ((a,b):ls, rs)
			(a, Right c) -> (ls, (a,c):rs)


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



-- | (Will be) used to parse the AEMO CSV files which contain daily data
parseAEMO :: ByteString -> Either String (Vector (String, String, String, Int, String, String, Double))
parseAEMO file =
	-- Removes the beginning and end lines of the file
	-- AEMO data files have two headers and a footer which causes issues when parsing
	-- Using intercalate instead of unlines to ensure that new lines are the same
	-- as the original document
	let trimmed = C.intercalate "\r\n" . init . drop 1 . C.lines $ file
	in decode HasHeader trimmed


