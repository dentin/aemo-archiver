{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}

module Main where

import           Data.ByteString.Lazy         (ByteString)
import qualified Data.ByteString.Lazy  as BSL (empty)

import           Data.Vector                  (Vector)
import qualified Data.Vector           as V   (length, mapM_)

import qualified Data.ByteString.Lazy.Char8 as C (intercalate, lines)
import qualified Data.HashSet          as S   (fromList, member)
import           Data.Text                    (Text, unpack)
import qualified Data.Text             as T   (pack)

import           Control.Exception            (SomeException, catch)
import           Data.Csv                     (HasHeader (..), decode)

import           Network.HTTP                 (Request, Response, HStream, getRequest, rspBody,
                                               getAuth, failHTTPS, openStream, host, port,
                                               normalizeRequest, defaultNormalizeRequestOptions,
                                               rqBody, rqURI, normDoClose, simpleHTTP_)
import           Network.Stream               (ConnError (..), Result)
import           Network.URI                  (parseURI, parseURIReference, relativeTo)
import           Text.HTML.TagSoup            (Tag (TagOpen), parseTags)

import           Control.Arrow                (second)
import           Control.DeepSeq              (NFData, deepseq)
import           Control.Monad                (forM_, unless, filterM)
import           Data.Char                    (toLower)
import           Data.Either                  (partitionEithers)
import           Data.List                    (isSuffixOf)
import           Data.List.Split              (chunksOf)
import           Data.Maybe                   (fromMaybe, mapMaybe, isNothing)

import           Codec.Archive.Zip            (toArchive, filesInArchive, findEntryByPath, fromEntry)

import           Control.Monad.IO.Class       (liftIO)
import           Control.Monad.Logger         (NoLoggingT)
import           Control.Monad.Trans.Resource (ResourceT)
import           Data.Time                    (ZonedTime, parseTime, zonedTimeToUTC)
import           Database.Persist             (insert)
import           Database.Persist.Sqlite      (SqlPersistT, runSqlite, runMigration, selectList, entityVal, selectFirst,
                                               (==.))
import           System.Locale                (defaultTimeLocale)

import           System.Directory             (doesFileExist)
import           System.IO                    (BufferMode (NoBuffering), hSetBuffering, stdout)

import           Data.String.Utils            (split)

import           AEMO.Types


type URL = String
type FileName = String

type CSVRow = ((), (), (), (), String, String, Double)


dbPath :: Text
dbPath = "AEMO.sqlite"

aemo5mPSURL :: URL
aemo5mPSURL =  "http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/"

aemoPSArchiveURL :: URL
aemoPSArchiveURL =  "http://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_SCADA/"


main :: IO ()
main = do
    hSetBuffering stdout NoBuffering

    -- TODO: there is a case to be made to always check the archive
    dbExists <- doesFileExist (unpack dbPath)

    -- Database
    runDB $ runMigration migrateAll

    -- Get the names of all known zip files in the database
    knownZipFiles <- allDbZips

    unless dbExists $
        fetchArchiveActualLoad knownZipFiles

    fetchDaily5mActualLoad knownZipFiles


fetchDaily5mActualLoad :: [Text] -> IO ()
fetchDaily5mActualLoad knownZipFiles = do
    putStrLn "Finding new 5 minute zips..."
    zipLinks <- joinLinks aemo5mPSURL
    retrieve knownZipFiles zipLinks


fetchArchiveActualLoad :: [Text] -> IO ()
fetchArchiveActualLoad knownZipFiles = do
    putStrLn "Finding new archive zips..."
    zipLinks <- joinLinks aemoPSArchiveURL
    retrieve knownZipFiles zipLinks


retrieve :: [Text] -> [(FileName, URL)] -> IO ()
retrieve knownZipFiles zipLinks = do
    -- Filter URLs for only those that haven't been inserted
    let seenfiles = S.fromList knownZipFiles
        unseen = filter (\(fn,_) -> not $ S.member (T.pack fn) seenfiles) zipLinks

    -- We're done if there aren't any files we haven't loaded yet
    unless (null unseen) $ do
        -- Fetch the contents of the zip files
        putStrLn ("Fetching " ++ show (length unseen) ++ " new files:")
        fetched <- fetchFiles unseen
        putChar '\n'
        let (ferrs, rslts) = partition' fetched
        unless (rslts `seq` null ferrs) $
            putStrLn "Fetch failures:" >> mapM_ print ferrs
        putStrLn ("Files fetched: " ++ show (length rslts))

        putStrLn "Processing data:"
        mapM_ (process 10) rslts
        putStrLn ""


filename :: URL -> String
filename url = last (split "/" url)


process :: Int -> (FileName, ByteString) -> IO ()
process c (fn, bs) =
    if c <= 0
        then do
            putStrLn ("Recursion limit reached for URL " ++ fn)
            return ()
        else do
            -- Extract zips from the archive zip files
            let (zeerrs, zextracted) = partitionEithers . extractFiles ".zip" $ [(fn, bs)]
            if zextracted `seq` null zeerrs
                then do
                    -- Recurse with any new zip files
                    mapM_ (process (c-1)) zextracted
                    runDB $ insert $ AemoZipFile (T.pack fn)
                    putStrLn ("\nProcessed " ++ show (length zextracted) ++ " archive zip files from file " ++ fn)
                else processCSVs (fn, bs)


processCSVs :: (FileName, ByteString) -> IO ()
processCSVs (fn, bs) = do
    -- Extract CSVs from the zip files
    let (eerrs, extracted) = partitionEithers . extractFiles ".csv" $ [(fn, bs)]
    unless (extracted `seq` null eerrs) $
        putStrLn "Extraction failures:" >> mapM_ print eerrs

    -- Check if CSV is already in db, to avoid new archive zips containing old current CVS files
    newCsvFiles <- filterM (\(f, _) -> csvNotInDb f) extracted

    -- Parse the CSV files into database types
    let (perrs, parsed) = partition' . map (second parseAEMO) $ newCsvFiles
    unless (parsed `seq` null perrs) $
        putStrLn "Parsing failures:" >> mapM_ print perrs

    -- Insert data into database
    mapM_ (runDB . insertCSV) parsed
    -- Insert zip file filename into database
    runDB $ insert $ AemoZipFile (T.pack fn)
    putChar '.'

    return ()


csvNotInDb :: FileName -> IO Bool
csvNotInDb f = do
    csv <- runDB $ selectFirst [AemoCsvFileFileName ==. T.pack f] []
    return (isNothing csv)


-- | Run the SQL on the sqlite filesystem path
runDB :: SqlPersistT (NoLoggingT (ResourceT IO)) a -> IO a
runDB = runSqlite dbPath


-- | Get the names of all known zip files in the database
allDbZips :: IO [Text]
allDbZips = runDB $ do
    es <- selectList [] []
    return $ map (aemoZipFileFileName . entityVal) es


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
    ruri <- parseURIReference relative
    return (filename relative, show (ruri `relativeTo` buri))


-- | Given a list of URLs, attempts to fetch them all and pairs the result with
--   the url of the request.
fetchFiles :: [(FileName, URL)] -> IO [(FileName, Either String ByteString)]
fetchFiles urls =
    mapM fetch urls where
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


-- | Parse the AEMO CSV files which contain daily data
parseAEMO :: ByteString -> Either String (Vector CSVRow)
parseAEMO file =
    -- Removes the beginning and end lines of the file
    -- AEMO data files have two headers and a footer which causes issues when parsing
    -- Using intercalate instead of unlines to ensure that new lines are the same
    -- as the original document
    let trimmed = C.intercalate "\r\n" . init . drop 1 . C.lines $ file
    in  decode HasHeader trimmed


-- | Takes a tuple parsed from the AEMO CSV data and produces a PSDatum. Time of recording is
-- parsed by appending the +1000 timezone to ensure the correct UTC time is parsed.
csvTupleToPSDatum :: AemoCsvFileId -> CSVRow -> Either String PSDatum
csvTupleToPSDatum fid (_D, _DISPATCH, _UNIT_SCADA, _1, dateStr, duid, val) = do
    -- TODO: fix the "+1000" timezone offset - first check if AEMO actually changes timezone, I guess otherwise this is fine...
    let mzt = parseTime defaultTimeLocale "%0Y/%m/%d %H:%M:%S%z" (dateStr ++ "+1000") :: Maybe ZonedTime
    case mzt of
        Nothing     -> Left $ "Failed to parse time \"" ++ dateStr ++ "\""
        Just zt     ->
            let utctime = zonedTimeToUTC zt
            in Right $ PSDatum (T.pack duid) utctime val fid


insertCSV :: (String, Vector CSVRow) -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
insertCSV (file, vec) = do
    fid <- insert $ AemoCsvFile (T.pack file) (V.length vec)
    V.mapM_ (ins fid) vec
    -- liftIO (putStrLn ("Inserted data from " ++ file))
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
    Right rsp -> rspBody rsp `deepseq` Right rsp

