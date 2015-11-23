{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module AEMO.Database where

-- import           Database.Persist.Sql         (SqlPersistT, entityVal,
--                                                runMigration, runSqlPersistMPool,
--                                                selectFirst, selectList, (==.))

import qualified Data.Text                    as T
-- import           Control.Monad.Logger         (NoLoggingT)
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Resource (ResourceT, runResourceT)
import           Control.Monad.Reader (runReaderT)

import           Data.Pool (withResource)


#if MIN_VERSION_time(1,5,0)
import           Data.Time                    (ZonedTime, zonedTimeToUTC)
import           Data.Time.Format             (defaultTimeLocale, parseTimeM)
#else
import           Data.Time                    (ZonedTime, parseTime,
                                               zonedTimeToUTC)
import           System.Locale                (defaultTimeLocale)
#endif

import           AEMO.Types

import           Control.Lens.Getter

import           Control.Monad.Logger

import           Database.Persist.Postgresql

import Data.String.Here


#if !MIN_VERSION_base(4,8,0)
import Data.Functor
#endif

type CSVRow = ((), (), (), (), String, String, Double)
-- type DBMonad a = SqlPersistT (NoLoggingT (ResourceT IO)) a
type DBMonad a = SqlPersistT (LoggingT (ResourceT IO)) a


migrateDb :: AppM ()
migrateDb = do
    runDB $ runMigration migrateAll
    updateLatestTimesSlow


updateLatestTimesSlow :: AppM ()
updateLatestTimesSlow = do
    Just conn <- use connPool
    withResource conn $ \sqlbknd -> do
        liftIO $ flip runReaderT sqlbknd $
            rawExecute [here|
                        BEGIN;
                           DELETE FROM latest_power_station_datum;
                           INSERT INTO latest_power_station_datum(duid,sample_time)
                                (SELECT ps.duid AS duid, max(sample_time) AS sample_time
                                FROM (SELECT DISTINCT duid FROM power_station) AS ps, power_station_datum AS psd
                                WHERE psd.duid = ps.duid
                                GROUP BY ps.duid);
                        COMMIT;
                        |]
                       []




csvNotInDb :: FileName -> DBMonad Bool
csvNotInDb f = (== 0) <$> count [AemoCsvFileFileName ==. T.pack f]

zipNotInDb :: FileName -> DBMonad Bool
zipNotInDb f = (== 0) <$> count [AemoZipFileFileName ==. T.pack f]


-- | Run the SQL on the sqlite filesystem path
runDB :: DBMonad a -> AppM a
runDB act = do
    mconn <- use connPool
    lggr <- askLoggerIO
    case mconn of
        -- Just conn -> liftIO $ runSqlPersistMPool act conn
        Just conn -> liftIO $ runResourceT $ flip runLoggingT lggr $ runSqlPool act conn
        Nothing -> error "runDB: no database connection found!"


-- | Takes a tuple parsed from the AEMO CSV data and produces a PowerStationDatum. Time of recording is
-- parsed by appending the +1000 timezone to ensure the correct UTC time is parsed.
csvTupleToPowerStationDatum :: AemoCsvFileId -> CSVRow -> Either String PowerStationDatum
csvTupleToPowerStationDatum fid (_D, _DISPATCH, _UNIT_SCADA, _1, dateStr, duid, val) = do
    -- TODO: fix the "+1000" timezone offset - first check if AEMO actually changes timezone,
    --       I guess otherwise this is fine...
    let
#if MIN_VERSION_time(1,5,0)
        parseTime = parseTimeM True
#endif
        mzt = parseTime defaultTimeLocale "%0Y/%m/%d %H:%M:%S%z" (dateStr ++ "+1000") :: Maybe ZonedTime
    case mzt of
        Nothing     -> Left $ "Failed to parse time \"" ++ dateStr ++ "\""
        Just zt     ->
            let utctime = zonedTimeToUTC zt
            in Right $ PowerStationDatum (T.pack duid) utctime val fid


