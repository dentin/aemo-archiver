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
import           Control.Monad.IO.Class
import           Control.Monad.Trans.State.Lazy
import 			 Control.Monad.State.Class
import 			 Control.Monad.Logger

import 			 Database.Persist.Postgresql

import 			 Control.Lens



type FileName = String

data AppState = AS {_connPool :: ConnectionPool}
$(makeLenses ''AppState)

newtype AppM a = AppM {runAppM :: StateT AppState (NoLoggingT IO) a}
	deriving (Functor, Applicative, Monad, MonadIO, MonadState AppState)


execApp :: AppState -> AppM a -> IO a
execApp st (AppM m) = runNoLoggingT $ evalStateT m st

execLoggerAppM :: AppState -> AppM a -> NoLoggingT IO a
execLoggerAppM st (AppM m) = evalStateT m st


share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
	PowerStation
		participant				Text
		stationName				Text
		region					Text
		dispatchType			Text
		category				Text
		classification			Text
		fuelSourcePrimary		Text Maybe
		fuelSourceDescriptor	Text Maybe
		techTypePrimary			Text Maybe
		techTypeDescriptor		Text Maybe
		physicalUnitNo			Text Maybe
		unitSizeMW				Text
		aggregation				Bool
		duid					Text
		regCapMW				Double Maybe
		maxCapMW				Double Maybe
		maxROCPerMin			Double Maybe

	AemoCsvFile
		fileName Text
		recordsInserted Int
		UniqueAEMOFile fileName

	AemoZipFile
		fileName Text

	PowerStationDatum
		duid Text
		sampleTime UTCTime
		megaWatt Double
		file AemoCsvFileId
		deriving Show

	|]
