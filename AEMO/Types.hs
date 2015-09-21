{-# LANGUAGE CPP                        #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

module AEMO.Types where

import           Data.Text                      (Text)
import           Data.Time                      (UTCTime)
import           Database.Persist.TH            (mkMigrate, mkPersist,
                                                 persistLowerCase, share,
                                                 sqlSettings)

#if !MIN_VERSION_base(4,8,0)
import           Control.Applicative
#endif
import           Control.Monad.Base
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import           Control.Monad.State.Class
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.State.Lazy


import           Database.Persist.Postgresql

import           Control.Lens                   hiding ((.=))

import qualified Data.ByteString.Char8          as B8

import           System.Log.FastLogger

import           Control.Exception              (SomeException, try)

import           Data.Csv

import           Data.Time.LocalTime

#if MIN_VERSION_time(1,5,0)
import           Data.Time.Format               (defaultTimeLocale, formatTime)
#else
import           Data.Time.Format               (formatTime)
import           System.Locale                  (defaultTimeLocale)
#endif




type FileName = String

data AppState = AS {_connPool     :: Maybe ConnectionPool
                    ,_logger      :: LogLevel -> Loc -> LogSource -> LogLevel -> LogStr -> IO ()
                    ,_minLogLevel :: LogLevel}
$(makeLenses ''AppState)

-- type AppM a = StateT AppState IO a
newtype AppM a = AppM {runAppM :: StateT AppState IO a}
    deriving (Functor, Applicative, Monad, MonadIO, MonadState AppState
             , MonadBase IO)


instance MonadLogger AppM where
    monadLoggerLog loc src lev msg = do
        lg <- use logger
        minLog <- use minLogLevel
        liftIO $ lg minLog loc src lev (toLogStr msg)

instance MonadLoggerIO AppM where
    askLoggerIO = do
        lg <- use logger
        minLog <- use minLogLevel
        return $ lg minLog


-- The following is voo doo stolen from
-- https://github.com/lfairy/haskol/blob/master/Web/KoL/Core.hs#L58
-- Don't ask me how this works, just know that it compiles and probably works.
instance MonadBaseControl IO AppM where
    type StM AppM a = StM (StateT AppState IO) a

    liftBaseWith f = AppM . liftBaseWith $ \runInBase -> f $ runInBase . runAppM
    restoreM     = AppM . restoreM
    {-# INLINABLE liftBaseWith #-}
    {-# INLINABLE restoreM #-}


execAppM :: AppState -> AppM a -> IO a
-- execApp st (AppM m) = runNoLoggingT $ evalStateT m st
execAppM st (AppM m) = evalStateT m st


runApp :: ConnectionString -> Int -> LogLevel -> AppM a -> IO (Either SomeException a)
runApp cstr nconn lev app = try $ do
    execAppM (AS Nothing makeLog lev) $ do
        withPostgresqlPool cstr nconn $ \conn -> do
            connPool ?= conn
            app

runAppPool :: ConnectionPool -> LogLevel -> AppM a -> IO (Either SomeException a)
runAppPool pool lev app = try $ do
    execAppM (AS (Just pool) makeLog lev) app


makeLog :: LogLevel -> Loc -> LogSource -> LogLevel -> LogStr -> IO ()
makeLog minLev loc src lev str = if lev >= minLev
    then B8.putStrLn . fromLogStr $ defaultLogStr loc src lev str
    else return ()


share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
    PowerStation
        participant             Text
        stationName             Text
        region                  Text
        dispatchType            Text
        category                Text
        classification          Text
        fuelSourcePrimary       Text Maybe
        fuelSourceDescriptor    Text Maybe
        techTypePrimary         Text Maybe
        techTypeDescriptor      Text Maybe
        physicalUnitNo          Text Maybe
        unitSizeMW              Text
        aggregation             Bool
        duid                    Text
        regCapMW                Text Maybe
        maxCapMW                Text Maybe
        maxROCPerMin            Text Maybe
        deriving Show

    AemoCsvFile
        fileName Text
        recordsInserted Int
        UniqueAemoFile fileName
        deriving Show

    AemoZipFile
        fileName Text
        deriving Show

    PowerStationDatum
        duid Text
        sampleTime UTCTime
        megaWatt Double
        file AemoCsvFileId
        deriving Show

    DuidLocation
        duid    Text
        lat     Double
        lon     Double
        comment Text
        UniqueDuidLocation duid
        deriving Show
    |]


instance ToNamedRecord PowerStation where
    toNamedRecord (PowerStation {..}) = namedRecord [
        "Participant"                  .= powerStationParticipant,
        "Station Name"                 .= powerStationStationName,
        "Region"                       .= powerStationRegion,
        "Dispatch Type"                .= powerStationDispatchType,
        "Category"                     .= powerStationCategory,
        "Classification"               .= powerStationClassification,
        "Fuel Source - Primary"        .= powerStationFuelSourcePrimary,
        "Fuel Source - Descriptor"     .= powerStationFuelSourceDescriptor,
        "Technology Type - Primary"    .= powerStationTechTypePrimary,
        "Technology Type - Descriptor" .= powerStationTechTypeDescriptor,
        "Physical Unit No."            .= powerStationPhysicalUnitNo,
        "Unit Size (MW)"               .= powerStationUnitSizeMW,
        "Aggregation"                  .= bToT powerStationAggregation,
        "DUID"                         .= powerStationDuid,
        "Reg Cap (MW)"                 .= powerStationRegCapMW,
        "Max Cap (MW)"                 .= powerStationMaxCapMW,
        "Max ROC/Min"                  .= powerStationMaxROCPerMin
        ]

instance ToNamedRecord PowerStationDatum where
    toNamedRecord (PowerStationDatum {..}) = namedRecord
        [ "DUID"        .= powerStationDatumDuid
        , "Sample Time (AEST)" .= formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%z"
                                                (utcToZonedTime aest powerStationDatumSampleTime)
        , "MW"          .= powerStationDatumMegaWatt
        ]

aest :: TimeZone
aest = TimeZone
            { timeZoneMinutes = 600
            , timeZoneSummerOnly = False
            , timeZoneName = "" -- "AEST"
            }

instance FromNamedRecord PowerStation where
    parseNamedRecord m =
        PowerStation
        <$> m .: "Participant"
        <*> m .: "Station Name"
        <*> m .: "Region"
        <*> m .: "Dispatch Type"
        <*> m .: "Category"
        <*> m .: "Classification"
        <*> m .: "Fuel Source - Primary"
        <*> m .: "Fuel Source - Descriptor"
        <*> m .: "Technology Type - Primary"
        <*> m .: "Technology Type - Descriptor"
        <*> m .: "Physical Unit No."
        <*> m .: "Unit Size (MW)"
        <*> (tToB <$> m .: "Aggregation")
        <*> m .: "DUID"
        <*> m .: "Reg Cap (MW)"
        <*> m .: "Max Cap (MW)"
        <*> m .: "Max ROC/Min"

bToT :: Bool -> Text
bToT b = if b then "Y" else "N"

tToB :: Text -> Bool
tToB t = case t of
    "N" -> False
    "Y" -> True
    _ -> error $ "temp-server.hs tToB: could not parse string: " ++ show t
