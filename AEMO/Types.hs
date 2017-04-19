{-# LANGUAGE CPP                        #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
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

import           Data.Text                      (Text, strip)
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

import           Data.Csv                       (FromNamedRecord, ToNamedRecord,
                                                 namedRecord)
import qualified Data.Csv                       as C

import           Data.Time.LocalTime

#if MIN_VERSION_time(1,5,0)
import           Data.Time.Format               (defaultTimeLocale, formatTime)
#else
import           Data.Time.Format               (formatTime)
import           System.Locale                  (defaultTimeLocale)
#endif

import           Configuration.Utils            hiding ((.=))
import qualified Configuration.Utils            as U

import qualified Network.Wreq                   as Wreq

import           Data.Monoid                    ((<>))

data DBConf = DBConf
  {_dbConnString  :: String
  ,_dbConnections :: Int
  }
$(makeLenses ''DBConf)

defaultDBConf :: DBConf
defaultDBConf = DBConf
  {_dbConnString = ""
  ,_dbConnections = 2
  }

instance FromJSON (DBConf -> DBConf) where
  parseJSON = withObject "DBConf" $ \o -> id
    <$< dbConnString  ..: "dbConnString" % o
    <*< dbConnections ..: "dbConnections" % o

instance ToJSON DBConf where
  toJSON a = object
    ["dbConnString"  U..= _dbConnString  a
    ,"dbConnections" U..= _dbConnections a
    ]

pDBConf :: MParser DBConf
pDBConf = id
  <$< dbConnString  .:: strOption    % short 'd' <> long "db-conn-string" <> metavar "CONNSTR"
      <> help "PostgreSQL connection string"
  <*< dbConnections .:: option auto  % short 'n' <> long "db-connections" <> metavar "INT"
      <> help "Number of database connections (minimum 2)"

data AEMOConf = AEMOConf
  { _aemoDB             :: DBConf
  , _aemoGensAndLoads   :: FilePath
  , _aemoStationLocs    :: FilePath
  , _aemoDryRun         :: Bool
  , _aemoUpdateStations :: Bool
  }
$(makeLenses ''AEMOConf)

defaultAemoConfig :: AEMOConf
defaultAemoConfig = AEMOConf
  {_aemoDB             = defaultDBConf
  ,_aemoGensAndLoads   = "power_station_metadata/nem-Generators and Scheduled Loads.csv"
  ,_aemoStationLocs    = "power_station_metadata/power_station_locations.csv"
  ,_aemoDryRun         = True
  ,_aemoUpdateStations = False
  }

instance FromJSON (AEMOConf -> AEMOConf) where
  parseJSON = withObject "AEMOConf" $ \o -> id
    <$< aemoDB             %.: "aemoDB"          % o
    <*< aemoGensAndLoads   ..: "generators-csv"  % o
    <*< aemoStationLocs    ..: "stations-csv"    % o
    <*< aemoDryRun         ..: "dry-run"         % o
    <*< aemoUpdateStations ..: "update-stations" % o

instance ToJSON AEMOConf where
  toJSON a = object
    ["aemoDB"             U..= _aemoDB a
    ,"aemoGensAndLoads"   U..= _aemoGensAndLoads a
    ,"aemoStationLocs"    U..= _aemoStationLocs a
    ,"aemoDryRun"         U..= _aemoDryRun a
    ,"aemoUpdateStations" U..= _aemoUpdateStations a
    ]

pAEMOConf :: MParser AEMOConf
pAEMOConf = id
  <$< aemoDB             %:: pDBConf
  <*< aemoGensAndLoads   .:: strOption    % short 'g' <> long "generators-csv" <> metavar "CSV" <> action "file"
    <> help "path to Generation and Loads CSV file"
  <*< aemoStationLocs    .:: strOption    % short 's' <> long "stations-csv"   <> metavar "CSV" <> action "file"
    <> help "path to power station locations CSV file"
  <*< aemoDryRun         .:: switch       % short 'D' <> long "dry-run"
    <> help "Don't actually perform DB modifications"
  <*< aemoUpdateStations .:: switch       % short 'u' <> long "update-stations"
    <> help "Update station locations"

type FileName = String

data AppState = AS {_connPool     :: Maybe ConnectionPool
                    ,_logger      :: LogLevel -> Loc -> LogSource -> LogLevel -> LogStr -> IO ()
                    ,_minLogLevel :: LogLevel
                    ,_httpOptions :: Wreq.Options}
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

    liftBaseWith f = AppM (liftBaseWith (\runInBase -> f (\x -> runInBase (runAppM x))))
    restoreM     = AppM . restoreM
    {-# INLINABLE liftBaseWith #-}
    {-# INLINABLE restoreM #-}


execAppM :: AppState -> AppM a -> IO a
-- execApp st (AppM m) = runNoLoggingT $ evalStateT m st
execAppM st (AppM m) = evalStateT m st


runApp :: ConnectionString -> Int -> LogLevel -> Wreq.Options -> AppM a -> IO (Either SomeException a)
runApp cstr nconn lev opts app = try $ do
    execAppM (AS Nothing makeLog lev opts) $ do
        withPostgresqlPool cstr nconn $ \conn -> do
            connPool ?= conn
            app

runAppPool :: ConnectionPool -> LogLevel -> Wreq.Options -> AppM a -> IO (Either SomeException a)
runAppPool pool lev opts app = try $ do
    execAppM (AS (Just pool) makeLog lev opts) app


makeLog :: LogLevel -> Loc -> LogSource -> LogLevel -> LogStr -> IO ()
makeLog minLev loc src lev lstr = if lev >= minLev
    then B8.putStrLn . fromLogStr $ defaultLogStr loc src lev lstr
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

    -- Also update AEMO.Database if this is changed.
    LatestDuidDatum sql=latest_power_station_datum
        duid        Text        id=duid
        sampleTime  UTCTime     id=sample_time
        UniqueLatestDuidTime duid
        deriving Show

    |]


instance ToNamedRecord PowerStation where
    toNamedRecord (PowerStation {..}) = namedRecord [
        "Participant"                  C..= strip powerStationParticipant,
        "Station Name"                 C..= strip powerStationStationName,
        "Region"                       C..= strip powerStationRegion,
        "Dispatch Type"                C..= strip powerStationDispatchType,
        "Category"                     C..= strip powerStationCategory,
        "Classification"               C..= strip powerStationClassification,
        "Fuel Source - Primary"        C..= fmap strip powerStationFuelSourcePrimary,
        "Fuel Source - Descriptor"     C..= fmap strip powerStationFuelSourceDescriptor,
        "Technology Type - Primary"    C..= fmap strip powerStationTechTypePrimary,
        "Technology Type - Descriptor" C..= fmap strip powerStationTechTypeDescriptor,
        "Physical Unit No."            C..= fmap strip powerStationPhysicalUnitNo,
        "Unit Size (MW)"               C..= strip powerStationUnitSizeMW,
        "Aggregation"                  C..= bToT powerStationAggregation,
        "DUID"                         C..= strip powerStationDuid,
        "Reg Cap (MW)"                 C..= fmap strip powerStationRegCapMW,
        "Max Cap (MW)"                 C..= fmap strip powerStationMaxCapMW,
        "Max ROC/Min"                  C..= fmap strip powerStationMaxROCPerMin
        ]

instance ToNamedRecord PowerStationDatum where
    toNamedRecord (PowerStationDatum {..}) = namedRecord
        [ "DUID"        C..= powerStationDatumDuid
        , "Sample Time (AEST)" C..= formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%z"
                                                (utcToZonedTime aest powerStationDatumSampleTime)
        , "MW"          C..= powerStationDatumMegaWatt
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
        <$> m C..: "Participant"
        <*> m C..: "Station Name"
        <*> m C..: "Region"
        <*> m C..: "Dispatch Type"
        <*> m C..: "Category"
        <*> m C..: "Classification"
        <*> m C..: "Fuel Source - Primary"
        <*> m C..: "Fuel Source - Descriptor"
        <*> m C..: "Technology Type - Primary"
        <*> m C..: "Technology Type - Descriptor"
        <*> m C..: "Physical Unit No."
        <*> m C..: "Unit Size (MW)"
        <*> (tToB <$> m C..: "Aggregation")
        <*> m C..: "DUID"
        <*> m C..: "Reg Cap (MW)"
        <*> m C..: "Max Cap (MW)"
        <*> m C..: "Max ROC/Min"

bToT :: Bool -> Text
bToT b = if b then "Y" else "N"

tToB :: Text -> Bool
tToB t = case t of
    "N" -> False
    "Y" -> True
    _   -> error $ "AEMO.Types.tToB: could not parse string: " ++ show t
