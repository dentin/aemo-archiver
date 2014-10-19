{-# LANGUAGE OverloadedStrings #-}

module Main where

import Text.HTML.TagSoup
import Network.HTTP
import qualified Data.Text as T
import           Data.Text (Text)
import qualified Data.ByteString as BS
import           Data.ByteString (ByteString)


main = putStrLn "Hello World"


getARefs :: String -> IO [Text]
getARefs url = do
	ersp <- simpleHTTP ((getRequest url) {rqBody = ("" :: Text)})
	case ersp of
		Left err -> print err >> return []
		Right rsp -> do
			let tags = parseTags (rspBody rsp)
			return [val | t@(TagOpen n attrs) <- tags, n `elem` ["a","A"],
						  a@(key,val) <- attrs, key `elem` ["href","HREF"]
						  ]

joinLinks :: String -> IO [Text]
joinLinks url = do
	links <- getARefs url
	return . filter (not . T.null) $ map addURI links where
		addURI txt = case T.uncons txt of
			Just ('/',rest) -> T.concat [T.pack url, txt]
			_ -> ""


