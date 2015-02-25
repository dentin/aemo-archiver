{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import           Data.ByteString.Lazy         (ByteString)
import qualified Data.ByteString.Lazy         as BSL


import           Data.Vector                  (Vector)
import qualified Data.Vector                  as V

import qualified Data.ByteString.Lazy.Char8   as C
import qualified Data.HashSet                 as S
import           Data.Text                    (Text, unpack)
import qualified Data.Text                    as T



import           Control.Concurrent.Async
import           Control.Exception            (SomeException, catch)
import           Data.Csv

import           Network.HTTP
import           Network.Stream               (ConnError (..), Result)
import           Network.URI
import           Text.HTML.TagSoup

import           Control.Applicative
import           Control.Arrow
import           Control.DeepSeq              (NFData, force)
import           Control.Monad                (forM_, unless)
import           Data.Char                    (toLower)
import           Data.Either                  (partitionEithers)
import           Data.List                    (isSuffixOf)
import           Data.List.Split              (chunksOf)
import           Data.Maybe                   (fromMaybe, mapMaybe)

import           Codec.Archive.Zip

import           Control.Monad.IO.Class       (liftIO)
import           Control.Monad.Logger         (NoLoggingT)
import           Control.Monad.Trans.Resource (ResourceT)
import           Data.Time
import           Database.Persist
import           Database.Persist.Sqlite
import           System.Locale

import           Control.Concurrent
import           Data.Time.Units
import           System.Directory             (doesFileExist)
import           System.IO

import           AEMO.Types


type URL = String

type CSVRow = ((), (), (), (), String, String, Double)


dbPath :: Text
dbPath = "AEMO.sqlite"

aemo5mPSURL :: URL
aemo5mPSURL =  "http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/"

aemoPSArchiveURL :: URL
aemoPSArchiveURL =  "http://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/"


runDB :: SqlPersistT (NoLoggingT (ResourceT IO)) a -> IO a
runDB = runSqlite dbPath


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
    dbExists <- doesFileExist (unpack dbPath)
    unless (dbExists) fetchArchiveActualLoad
    fetchDaily5mActualLoad


fetchDaily5mActualLoad :: IO ()
fetchDaily5mActualLoad = do
    -- Get the names of all zip files from AEMO website
    zipLinks <- joinLinks aemo5mPSURL

    -- Get the names of all known zip files in the database
    knownZipFiles <- runDB $ do
        runMigration migrateAll

        es <- selectList [] []
        return $ map (aemoZipFileFileName . entityVal) es

    -- Filter URLs for only those that haven't been inserted
    let seenfiles = S.fromList knownZipFiles
        unseen = filter (\u -> not $ S.member (T.pack u) seenfiles) $ zipLinks

    -- We're done if there aren't any files we haven't loaded yet
    unless (null unseen) $ do
        putStrLn "\nFetching new files:"
        mapM_ putStrLn unseen

        -- Fetch the contents of the zip files
        fetched <- fetchFiles unseen
        let (ferrs,rslts) = partition' fetched
        if rslts `seq` null ferrs
            then return ()
            else putStrLn "Fetch failures:" >> mapM_ print ferrs
        unless (null rslts) $ do
            putStr "Files fetched: "
            print (length rslts)

        -- Extract CSVs from the zip files
        let (eerrs, extracted) = partitionEithers . extractFiles ".csv" $ rslts
        if extracted `seq` null eerrs
            then putStrLn ("Extracted " ++ show (length extracted) ++ " csv files.")
            else putStrLn "Extraction failures:" >> mapM_ print eerrs

        -- Parse the CSV files into database types
        let (perrs, parsed) = partition' . map (second parseAEMO) $ extracted
        if parsed `seq` null perrs
            then putStrLn ("Parsed " ++ show (length parsed) ++ " csv files.")
            else putStrLn "Parsing failures:" >> mapM_ print perrs

        -- Insert data into database
        mapM_ (runDB . insertCSV) parsed
        -- Insert zip file URLs into database
        forM_ rslts $ \(url,_) -> do
            runDB $ insert $ AemoZipFile (T.pack url)
        putStrLn ("Inserted data from " ++ show (length parsed) ++ " CSV files.")


fetchArchiveActualLoad :: IO ()
fetchArchiveActualLoad = do
    -- Get the names of all archive zip files from AEMO website
    zipLinks <- joinLinks aemoPSArchiveURL

    -- Get the names of all known zip files in the database
    knownZipFiles <- runDB $ do
        runMigration migrateAll

        es <- selectList [] []
        return $ map (aemoZipFileFileName . entityVal) es

    -- Filter URLs for only those that haven't been inserted
    let seenfiles = S.fromList knownZipFiles
        unseen = filter (\u -> not $ S.member (T.pack u) seenfiles) $ zipLinks

    -- We're done if there aren't any files we haven't loaded yet
    unless (null unseen) $ do
        putStrLn "\nFetching new files:"
        mapM_ putStrLn unseen

        -- Fetch the contents of the zip files
        fetched <- fetchFiles unseen
        let (ferrs,rslts) = partition' fetched
        if rslts `seq` null ferrs
            then return ()
            else putStrLn "Fetch failures:" >> mapM_ print ferrs
        unless (null rslts) $ do
            putStr "Files fetched: "
            print (length rslts)

        -- Extract zips from the archive zip files
        let (zeerrs, zextracted) = partitionEithers . extractFiles ".zip" $ rslts
        if zextracted `seq` null zeerrs
            then putStrLn ("Extracted " ++ show (length zextracted) ++ " archive zip files.")
            else putStrLn "Extraction failures:" >> mapM_ print zeerrs

        -- Extract CSVs from the zip files
        let (eerrs, extracted) = partitionEithers . extractFiles ".csv" $ zextracted
        if extracted `seq` null eerrs
            then putStrLn ("Extracted " ++ show (length extracted) ++ " csv files.")
            else putStrLn "Extraction failures:" >> mapM_ print eerrs

        -- Parse the CSV files into database types
        let (perrs, parsed) = partition' . map (second parseAEMO) $ extracted
        if parsed `seq` null perrs
            then putStrLn ("Parsed " ++ show (length parsed) ++ " csv files.")
            else putStrLn "Parsing failures:" >> mapM_ print perrs

        -- Insert data into database
        mapM_ (runDB . insertCSV) parsed
        -- Insert zip file URLs into database
        forM_ rslts $ \(url,_) -> do
            runDB $ insert $ AemoZipFile (T.pack url)
        putStrLn ("Inserted data from " ++ show (length parsed) ++ " CSV files.")


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
-- TODO: handle case insensitive matching of the .zip suffix
joinLinks :: URL -> IO [URL]
joinLinks url = do
    links <-getARefs url
    return . filter (isSuffixOf ".zip" . map toLower) . mapMaybe (joinURIs url) $ links


-- | Takes a base URL and a path relative to that URL and joins them:
--      joinURIs "http://example.com/foo/bar" "/baz/quux.txt" -> Just "http://example.com/baz/quux.txt"
--      joinURIs "http://example.com/foo/bar" "baz/quux.txt"  -> Just "http://example.com/foo/baz/quux.txt"
--      joinURIs "http://example.com/foo/bar/" "baz/quux.txt" -> Just "http://example.com/foo/bar/baz/quux.txt"
joinURIs :: String -> String -> Maybe String
joinURIs base relative = do
    buri <- parseURI         base
    ruri <- parseURIReference relative
    return $ show (ruri `relativeTo` buri)


-- | Given a list of URLs, attempts to fetch them all and pairs the result with
--   the url of the request. It performs fetches concurrently in groups of 40
fetchFiles :: [URL] -> IO [(URL,Either String ByteString)]
fetchFiles urls =
    concat <$> mapM (fmap force . mapConcurrently fetch) (chunksOf 40 urls) where
        fetch url = do
            res <- simpleHTTPSafe ((getRequest url) {rqBody = BSL.empty})
                    `catch` (\e -> return$ Left (ErrorMisc (show (e :: SomeException))))
            return $! (url,) $! case res of
                Right bs -> Right . rspBody $! bs
                Left err -> Left . show $! err


-- | Takes URLs and zip files and extracts all CSV files from each zip file
extractFiles :: String -> [(URL,ByteString)] -> [Either (URL,String) (String,ByteString)]
extractFiles suf arcs = concatMap extract arcs where
    extract (url,bs) =
        let
            arc = toArchive bs
            paths = filesInArchive arc
            csvs = filter (isSuffixOf suf . map toLower) paths
        in case csvs of
            []      -> [Left $ (url,"No " ++ suf ++ " found in " ++ url)]
            fs  -> map ext fs where
                ext f = case findEntryByPath f arc of
                    Nothing -> Left  (url, concat ["Could not find ", f, " in archive ", url])
                    Just e  -> Right (f, fromEntry e)


-- | (Will be) used to parse the AEMO CSV files which contain daily data
parseAEMO :: ByteString -> Either String (Vector CSVRow)
parseAEMO file =
    -- Removes the beginning and end lines of the file
    -- AEMO data files have two headers and a footer which causes issues when parsing
    -- Using intercalate instead of unlines to ensure that new lines are the same
    -- as the original document
    let trimmed = C.intercalate "\r\n" . init . drop 1 . C.lines $ file
    in decode HasHeader trimmed


-- | Takes a tuple parsed from the AEMO CSV data and produces a PSDatum. Time of recording is
-- parsed by appending the +1000 timezone to ensure the correct UTC time is parsed.
csvTupleToPSDatum :: AemoCsvFileId -> CSVRow -> Either String PSDatum
csvTupleToPSDatum fid (_D, _DISPATCH, _UNIT_SCADA, _1, dateStr, duid, val) = do
    -- TODO: fix the "+1000" timezone offset
    let mzt = parseTime defaultTimeLocale "%0Y/%m/%d %H:%M:%S%z" (dateStr ++ "+1000") :: Maybe ZonedTime
    case mzt of
        Nothing     -> Left $ "Failed to parse time \"" ++ dateStr ++ "\""
        Just zt     ->
            let utctime = zonedTimeToUTC zt
            in Right $ PSDatum (T.pack duid) utctime val fid


insertCSV :: (String, Vector CSVRow) -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
insertCSV (file,vec) = do
    fid <- insert $ AemoCsvFile (T.pack file) (V.length vec)
    V.mapM_ (ins fid) vec
    liftIO (putStrLn ("Inserted data from " ++ file))
    where
        ins fid r = case csvTupleToPSDatum fid r of
            Left str -> fail str
            Right psd -> insert psd


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
    Right rsp -> rspBody rsp `seq` Right rsp






