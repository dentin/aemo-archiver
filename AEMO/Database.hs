{-# LANGUAGE OverloadedStrings #-}

module AEMO.Database where

import           Database.Persist             (insert)
import           Database.Persist.Sqlite      (SqlPersistT, runSqlite, runMigration, selectList, entityVal, selectFirst,
                                               (==.))
import           Data.Text                    (Text)
import qualified Data.Text             as T   (pack)
import           Data.Maybe                   (isNothing)
import           Control.Monad.Logger         (NoLoggingT)
import           Control.Monad.Trans.Resource (ResourceT)
import           Data.Vector                  (Vector)
import qualified Data.Vector           as V   (length, mapM_)
import           Data.Time                    (ZonedTime, parseTime, zonedTimeToUTC)
import           System.Locale                (defaultTimeLocale)

import           AEMO.Types


type CSVRow = ((), (), (), (), String, String, Double)


dbPath :: Text
dbPath = "AEMO.sqlite"


--dbInsert :: types are hard!
dbInsert v = runDB $ insert v


migrateDb :: IO ()
migrateDb = runDB $ runMigration migrateAll


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


insertCSV :: (String, Vector CSVRow) -> IO ()
insertCSV (file, vec) = runDB $ do
    fid <- insert $ AemoCsvFile (T.pack file) (V.length vec)
    V.mapM_ (ins fid) vec
    -- liftIO (putStrLn ("Inserted data from " ++ file))
    where
        ins fid r = case csvTupleToPowerStationDatum fid r of
            Left str -> fail str
            Right psd -> insert psd


-- | Takes a tuple parsed from the AEMO CSV data and produces a PowerStationDatum. Time of recording is
-- parsed by appending the +1000 timezone to ensure the correct UTC time is parsed.
csvTupleToPowerStationDatum :: AemoCsvFileId -> CSVRow -> Either String PowerStationDatum
csvTupleToPowerStationDatum fid (_D, _DISPATCH, _UNIT_SCADA, _1, dateStr, duid, val) = do
    -- TODO: fix the "+1000" timezone offset - first check if AEMO actually changes timezone, I guess otherwise this is fine...
    let mzt = parseTime defaultTimeLocale "%0Y/%m/%d %H:%M:%S%z" (dateStr ++ "+1000") :: Maybe ZonedTime
    case mzt of
        Nothing     -> Left $ "Failed to parse time \"" ++ dateStr ++ "\""
        Just zt     ->
            let utctime = zonedTimeToUTC zt
            in Right $ PowerStationDatum (T.pack duid) utctime val fid


