{-# LANGUAGE OverloadedStrings #-}

module AEMO.Database where

-- import           Database.Persist.Sql         (SqlPersistT, entityVal,
--                                                runMigration, runSqlPersistMPool,
--                                                selectFirst, selectList, (==.))

import           Data.Maybe                   (isNothing)
import           Data.Text                    (Text)
import qualified Data.Text                    as T (pack)
-- import           Control.Monad.Logger         (NoLoggingT)
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Resource (ResourceT)
import           Data.Time                    (ZonedTime, parseTime,
                                               zonedTimeToUTC)

import           System.Locale                (defaultTimeLocale)

import           AEMO.Types

import           Control.Lens.Getter

import           Control.Monad.Logger

import           Database.Persist.Postgresql

import qualified Data.ByteString      as B


dbConn :: IO ConnectionString
dbConn = do
    conn <- B.readFile "aemo_database.conf"
    return conn
    -- "host=localhost dbname=aemoarchiver user=aemoarchiver password=weakpass port=5432"

type CSVRow = ((), (), (), (), String, String, Double)
type DBMonad a = SqlPersistT (NoLoggingT (ResourceT IO)) a


migrateDb :: AppM ()
migrateDb = runDB $ runMigration migrateAll


csvNotInDb :: FileName -> AppM Bool
csvNotInDb f = do
    csv <- runDB $ selectFirst [AemoCsvFileFileName ==. T.pack f] []
    return (isNothing csv)


-- | Run the SQL on the sqlite filesystem path
runDB :: DBMonad a-> AppM a
runDB act = do
    mconn <- use connPool
    case mconn of
        Just conn -> liftIO $ runSqlPersistMPool act conn
        Nothing -> error "runDB: no database connection found!"


-- | Get the names of all known zip files in the database
allDbZips :: AppM [Text]
allDbZips = runDB $ do
    es <- selectList [] []
    return $ map (aemoZipFileFileName . entityVal) es




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


