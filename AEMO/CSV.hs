{-# LANGUAGE OverloadedStrings #-}

module AEMO.CSV where

import           Data.ByteString.Lazy        (ByteString)
-- import qualified Data.ByteString.Lazy        as B
import           Data.Vector                 (Vector)
-- TODO: why Char8?
import           Control.Arrow               (second)
import           Control.Monad               (filterM, unless)
import qualified Data.ByteString.Lazy.Char8  as C (intercalate, lines)
import           Data.Csv                    (HasHeader (..), decode)
import           Data.Either                 (partitionEithers)
import qualified Data.HashSet                as S (fromList, member)
import           Data.Text                   (Text)
import qualified Data.Text                   as T (pack)
-- import           Data.List.Split              (chunksOf)

import           Control.Monad.IO.Class      (liftIO)

import           Database.Persist.Postgresql

import           AEMO.Database
import           AEMO.Types
import           AEMO.WebScraper

fetchDaily5mActualLoad :: [Text] -> AppM ()
fetchDaily5mActualLoad knownZipFiles = do
    liftIO $ putStrLn "Finding new 5 minute zips..."
    zipLinks <- liftIO $ joinLinks aemo5mPSURL
    retrieve knownZipFiles zipLinks


fetchArchiveActualLoad :: [Text] -> AppM ()
fetchArchiveActualLoad knownZipFiles = do
    liftIO $ putStrLn "Finding new archive zips..."
    zipLinks <- liftIO $ joinLinks aemoPSArchiveURL
    retrieve knownZipFiles zipLinks


retrieve :: [Text] -> [(FileName, URL)] -> AppM ()
retrieve knownZipFiles zipLinks = do
    -- Filter URLs for only those that haven't been inserted
    let seenfiles = S.fromList knownZipFiles
        unseen = filter (\(fn,_) -> not $ S.member (T.pack fn) seenfiles) zipLinks

    -- We're done if there aren't any files we haven't loaded yet
    unless (null unseen) $ do
        -- Fetch the contents of the zip files
        results <- liftIO $ do
            putStrLn ("Fetching " ++ show (length unseen) ++ " new files:")
            fetched <- fetchFiles unseen
            putChar '\n'
            let (ferrs, rslts) = partition' fetched
            unless (rslts `seq` null ferrs) $
                putStrLn "Fetch failures:" >> mapM_ print ferrs
            putStrLn ("Files fetched: " ++ show (length rslts))
            return rslts

        liftIO $ putStrLn "Processing data:"
        mapM_ (process 10) results
        liftIO $ putStrLn ""


process :: Int -> (FileName, ByteString) -> AppM ()
process c (fn, bs) =
    if c <= 0
        then do
            liftIO $ putStrLn ("Recursion limit reached for URL " ++ fn)
            return ()
        else do
            -- Extract zips from the archive zip files
            let (zeerrs, zextracted) = partitionEithers . extractFiles ".zip" $ [(fn, bs)]
            if zextracted `seq` null zeerrs
                then do
                    -- Recurse with any new zip files
                    mapM_ (process (c-1)) zextracted
                    runDB $ insert $ AemoZipFile (T.pack fn)
                    liftIO $ putStrLn ("\nProcessed " ++ show (length zextracted) ++ " archive zip files from file " ++ fn)
                else processCSVs (fn, bs)


processCSVs :: (FileName, ByteString) -> AppM ()
processCSVs (fn, bs) = do
    -- Extract CSVs from the zip files
    let (eerrs, extracted) = partitionEithers . extractFiles ".csv" $ [(fn, bs)]
    unless (extracted `seq` null eerrs) $
        liftIO $ putStrLn "Extraction failures:" >> mapM_ print eerrs

    -- Check if CSV is already in db, to avoid new archive zips containing old current CVS files
    newCsvFiles <- filterM (\(f, _) -> csvNotInDb f) extracted

    -- Parse the CSV files into database types
    let (perrs, parsed) = partition' . map (second parseAEMO) $ newCsvFiles
    unless (parsed `seq` null perrs) $
        liftIO $ putStrLn "Parsing failures:" >> mapM_ print perrs

    runDB $ do
        -- Insert data into database
        mapM_ insertCSV parsed
        -- Insert zip file filename into database
        insert $ AemoZipFile (T.pack fn)
        liftIO $ putChar '.'

    return ()


-- | Parse the AEMO CSV files which contain daily data
parseAEMO :: ByteString -> Either String (Vector CSVRow)
parseAEMO file =
    -- Removes the beginning and end lines of the file
    -- AEMO data files have two headers and a footer which causes issues when parsing
    -- Using intercalate instead of unlines to ensure that new lines are the same
    -- as the original document
    let trimmed = C.intercalate "\r\n" . init . drop 1 . C.lines $ file
    in  decode HasHeader trimmed


partition' :: [(a, Either b c)] -> ([(a,b)], [(a,c)])
partition' ps = go ps  where
    go [] = ([],[])
    go (x:xs) =
        let (ls,rs) = go xs
        in case x of
            (a, Left b ) -> ((a,b):ls, rs)
            (a, Right c) -> (ls, (a,c):rs)


