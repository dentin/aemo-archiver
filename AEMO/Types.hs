{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module AEMO.Types where

import           Data.Text                      (Text)
import           Data.Time                      (UTCTime)
import           Database.Persist.TH            (mkMigrate, mkPersist,
                                                 persistLowerCase, share,
                                                 sqlSettings)

import           Control.Applicative
import           Control.Monad.Base
import           Control.Monad.IO.Class
import           Control.Monad.Logger
import           Control.Monad.State.Class
import           Control.Monad.Trans.Control
import           Control.Monad.Trans.State.Lazy


import           Database.Persist.Postgresql

import           Control.Lens

import qualified Data.ByteString                as B

import           System.Log.FastLogger



type FileName = String

data AppState = AS {_connPool :: Maybe ConnectionPool,
                    _logger   :: Loc -> LogSource -> LogLevel -> LogStr -> IO ()}
$(makeLenses ''AppState)

newtype AppM a = AppM {runAppM :: StateT AppState IO a}
    deriving (Functor, Applicative, Monad, MonadIO, MonadState AppState
             , MonadBase IO)


instance MonadLogger AppM where
    monadLoggerLog loc src lev msg = do
        lg <- use logger
        liftIO $ lg loc src lev (toLogStr msg)

instance MonadLoggerIO AppM where
    askLoggerIO = use logger

-- The following is voo doo stolen from
-- https://github.com/lfairy/haskol/blob/master/Web/KoL/Core.hs#L58
instance MonadBaseControl IO AppM where
    newtype StM AppM a = AStM { unAStM :: StM (StateT AppState IO) a}

    liftBaseWith f = AppM . liftBaseWith $ \runInBase -> f $ fmap AStM . runInBase . runAppM
    restoreM     = AppM . restoreM . unAStM
    {-# INLINABLE liftBaseWith #-}
    {-# INLINABLE restoreM #-}


execAppM :: AppState -> AppM a -> IO a
-- execApp st (AppM m) = runNoLoggingT $ evalStateT m st
execAppM st (AppM m) = evalStateT m st

makeLog :: Loc -> LogSource -> LogLevel -> LogStr -> IO ()
makeLog loc src lev str = if lev >= LevelDebug
    then B.putStrLn . fromLogStr $ defaultLogStr loc src lev str
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
        regCapMW                Double Maybe
        maxCapMW                Double Maybe
        maxROCPerMin            Double Maybe

    AemoCsvFile
        fileName Text
        recordsInserted Int
        UniqueAemoFile fileName

    AemoZipFile
        fileName Text

    PowerStationDatum
        duid Text
        sampleTime UTCTime
        megaWatt Double
        file AemoCsvFileId
        deriving Show

    |]
