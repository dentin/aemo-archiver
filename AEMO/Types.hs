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

import           Database.Persist
import           Database.Persist.Sqlite
import 			 Database.Persist.TH

import           Data.Text (Text)
import qualified Data.Text as T

import Data.Time


share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
	PowerStation
		code Text
		name Text
		UniquePSCode code
	PSDatum
		code PowerStationId
		sampleTime UTCTime
		kiloWattHours Double
	|]