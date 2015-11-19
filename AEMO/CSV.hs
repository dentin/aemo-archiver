{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell   #-}

module AEMO.CSV where

import           Data.ByteString.Lazy        (ByteString)
-- import qualified Data.ByteString.Lazy        as B
import           Data.Vector                 (Vector)
import qualified Data.Vector                 as V

import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict             as M


import           Control.Monad               (filterM, unless, void, when)
import qualified Data.ByteString.Lazy.Char8  as C (intercalate, lines)
import           Data.Csv                    (HasHeader (..), decode)
import           Data.Either                 (partitionEithers)
import           Data.List                   (sort)

import           Data.Text                   (Text)
import qualified Data.Text                   as T (pack)
-- import           Data.List.Split              (chunksOf)


import           Data.Char                   (toLower)
import           Data.Function               (on)
import           Data.List                   (isSuffixOf)

import           Control.Monad.IO.Class      (liftIO)

import           Database.Persist.Postgresql

import           Control.Monad.Logger

import           AEMO.Database
import           AEMO.Types
import           AEMO.WebScraper
import           AEMO.ZipTree

fetchDaily5mActualLoad :: AppM Int
fetchDaily5mActualLoad = do
    $(logInfo) "Finding new 5 minute zips..."
    zipLinks <- liftIO $ joinLinks aemo5mPSURL
    retrieve 0 zipLinks


fetchArchiveActualLoad :: AppM Int
fetchArchiveActualLoad = do
    $(logInfo) "Finding new archive zips..."
    zipLinks <- liftIO $ joinLinks aemoPSArchiveURL
    retrieve 1 zipLinks


retrieve :: Int -> [(FileName, URL)] -> AppM Int
retrieve depth zipLinks = do

    -- Fetch the contents of the zip files

    unseen <- runDB $ filterM (zipNotInDb . fst) zipLinks

    $(logInfo) $ T.pack ("Fetching " ++ show (length unseen) ++ " new files:")
    fetched <- liftIO $ fetchFiles unseen
    $(logInfo) "Done fetching new files"
    let (ferrs, rslts) = partition' fetched
    unless (rslts `seq` null ferrs) $ do
        $(logWarn) "Fetch failures:"
        mapM_ ($(logInfo) . T.pack . show) ferrs
    $(logInfo) $ T.pack ("Files fetched: " ++ show (length rslts))

    $(logInfo) "Processing data:"

    case reverse (sort rslts) of
        [] -> pure ()
        (latest:srslts) -> do
            mapM_ (proc False depth) srslts
            proc True depth latest

    $(logInfo) "Done processing data"
    return (length unseen)


proc :: Bool -> Int -> (FileName,ByteString) -> AppM ()
proc updateLatest depth pair = case toZipTree depth pair of
    Left str -> $(logError) $ T.pack $ "Failed to extract data from fip file: " ++ fst pair ++ "\"" ++ str ++ "\""
    Right zt -> runDB $ insertZipTree updateLatest zt


insertZipTree :: Bool -> ZipTree ByteString -> DBMonad ()
insertZipTree updateLatest = void . travseseWithParentAndName
    (\fp -> insert (AemoZipFile (T.pack fp)) >> return fp)
    (\fp -> do
        let msg = "found zip file when only files were expected: " ++ fp
        $(logError) $ T.pack $ msg
        fail msg
    )
    (\parent name bs -> do
        when (isSuffixOf ".csv" . map toLower $ name) $ do
            unknownCsv <- csvNotInDb name
            when unknownCsv $ case parseAEMO bs of
                Left str -> fail str
                Right vec -> do
                    fid <- insert $ AemoCsvFile (T.pack name) (V.length vec)
                    let (errs,psdata) = partitionEithers . map (csvTupleToPowerStationDatum fid) . V.toList $ vec
                    case errs of
                        [] -> do
                            when updateLatest $ updateLatestDuidTimes psdata
                            insertMany_  psdata
                            $(logInfo) $ T.pack $ "Inserted data from " ++ show parent
                        _  -> do
                            $(logError) "Failed to parse CSV"
                            mapM_ ($(logError) . T.pack . show) errs
                            fail "Failed to parse CSVs"
        )


updateLatestDuidTimes :: [PowerStationDatum] -> DBMonad ()
updateLatestDuidTimes psdata = do
    latests <- selectList [] [Asc LatestDuidDatumDuid]
    let latestMap :: Map Text LatestDuidDatum
        latestMap = M.fromAscList
                    . map (\e -> (latestDuidDatumDuid . entityVal $ e, entityVal e))
                    $ latests

        updates :: Map Text LatestDuidDatum
        updates = M.fromList
                  . map (\d -> (powerStationDatumDuid d
                                , LatestDuidDatum (powerStationDatumDuid d)
                                                  (powerStationDatumSampleTime d))
                         )
                  $ psdata

        updated = M.unionWithKey (\duid ldda lddb
                                -> LatestDuidDatum duid (on max latestDuidDatumSampleTime ldda lddb))
                                latestMap
                                updates
    _ <- traverse (\ldd -> upsert ldd []) updated
    pure ()

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


