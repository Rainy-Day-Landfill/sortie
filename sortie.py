#!/usr/bin/env python3

# config
import configparser
from os import path, mkdir, walk
import distutils.util
import boto3
from botocore import exceptions
from enum import Enum
from datetime import datetime
from sys import stderr
import json
from jinja2 import Template
from tinytag import TinyTag
import shutil
import uuid

# Various exceptions specific to this script
# -
# Base exception
class FatalException( Exception ):
    """Base class for exceptions."""
    def __init__( self, expression, message ):
        pass


# Config is missing
class ConfigFileNotPresent( FatalException ):
    def __init__( self, expression, message ):
        super().__init__( expression, message )


# Config is missing a value that we were looking for
class ConfigMissingKey( FatalException ):
    def __init__( self, expression, message ):
        super().__init__( expression, message )


# We tried to use an AWS CLI profile that didn't exist.
class ConfigMissingAWSCLIProfile( FatalException ):
    def __init__( self, expression, message ):
        super().__init__( expression, message )


# We tried to use a feature not yet implemented
class FeatureNotImplemented( FatalException ):
    def __init__( self, expression, message ):
        super().__init__( expression, message )


# Don't have the proper permissions to perform that action
class InvalidPermissions( FatalException ):
    def __init__( self, expression, message ):
        super().__init__( expression, message )


# User didn't read the instructions
class InvalidValue( FatalException ):
    def __init__( self, expression, message ):
        super().__init__( expression, message )


# An object for reading the config file
class Config:
    def __init__( self, filepath ):
        # The config parser object to do the things.
        self.parser = configparser.ConfigParser()

        # The path to the file
        self.path = filepath

        # Check if the configuration file can be found, if not, we can't continue.
        if not path.exists( self.path ):
            raise ConfigFileNotPresent("Supplied config file not found: {0}".format( filepath ), "Bad Config File Path")

        # Read the path to the config file specified.
        self.parser.read( filepath )

        # use self as the access interface for values in the ini file

        # assumes the user is using profiles set with the aws cli
        self.environment = self.sane_get( 'aws', 'environment' )

        # the bucket name to work with
        self.bucket = self.sane_get( 'bucket', 'name' )

        # Note on typecast pattern used here:
        #   While configparser's interface has specific get methods associated with expected types, since python doesn't
        #   do Templates like, say, C++, it's actually quicker to sanitize input by keeping the sanitization method type
        #   -insensitive and typecasting from String values on local member assignment where necessary.  This also keeps
        #   the sane_get method's purpose clear -- to retrieve known values from the config file by their handle and
        #   raise an easy to read exception when that expected setting is not present.

        # whether or not to log to file
        self.log_to_file = self.str2bool( self.sane_get( 'logging', 'log_to_file' ) )

        # the maximum logging level
        self.max_logging_level = int( self.sane_get( 'logging', 'logging_level' ) )

        # the log file to use
        # only required if the logging.log_to_file option is set to True
        if self.log_to_file:
            self.log_file = self.sane_get( 'logging', 'log_file' )

        # targeting
        self.ingestion_mode = self.sane_get( 'ingestion', 'mode' )
        if self.ingestion_mode == 'track_list':
            self.track_list = self.sane_get( 'ingestion', 'track_list' )

        # the working directory to cache files into for processing
        self.cache_dir = self.sane_get( 'cache', 'directory' )
        if not path.exists( self.cache_dir ):
            mkdir( self.cache_dir )

        # the mask to use for target schema for S3 uploads
        self.sort_mask = self.sane_get( 'targeting', 'sort_mask' )

        # boolean indicator of whether or not to delete files in the s3 bucket after it uploads
        self.clean_up = self.str2bool( self.sane_get( 'targeting', 'clean_up' ) )

        # should the cache persist between runs?
        self.persistent_cache = self.str2bool( self.sane_get( 'cache', 'persistent' ) )

    # helper method to convert strings to bools
    def str2bool( self, val ):
        return bool( distutils.util.strtobool( val ) )

    # additional sanity checking to augment configparser
    def sane_get( self, header, key ):
        # try to fetch the header/key from the ini file
        try:
            val = self.parser[header][key]

        # if key not found, it means that header/key wasn't there.
        except KeyError:
            # raising our own exception with more readable output to make this easier to troubleshoot
            raise ConfigMissingKey(
                "expression_string Key ['{0}']['{1}'] not present in '{2}'.".format(
                    header,
                    key,
                    self.path
                ),
                # Assigning blame where blame is due
                "User Configuration Error"
            )
        # return the value to the calling method
        return val


# logging module

# Returns a timestamp in ISO 8601 format
# Reference: https://www.iso.org/iso-8601-date-and-time-format.html
def get8601():
    return datetime.now().isoformat()


# Create "verbosity channels" for logging.
class ERR( Enum ):
    FATAL = 1
    INFO  = 2
    WARN  = 3
    DEBUG = 4


class Logger():
    def __init__( self, mask_name, config ):
        # the application name
        self.mask_name = mask_name

        # the verbosity level to use
        self.verbosity = config.max_logging_level

        # boolean flag for whether to use a log file
        self.log_to_file = config.log_to_file

        # path to log file
        self.log_file = config.log_file

    def write_logfile( self, msg ):
        if self.log_to_file:
            with open( self.log_file, 'a+' ) as LF:
                LF.write( "{0}\n".format( msg ) )

    def timestamp_msg( self, msg ):
        return

    # log()
    # -
    # used by modules to print to file and log
    # params:
    # err_class - the channel to use for log messages
    # msg       - the content of the log message
    def log( self, err_class, msg ):
        # prepend 8601 format timestamp and mask name
        msg = "[{0}]\t[{1}]\t[{2}] {3}".format(
            get8601(),
            err_class.name,
            self.mask_name,
            msg
        )

        # PEP8 is not used here deliberately.  I have a rant about PEP8.
        if err_class == ERR.INFO and self.verbosity >= err_class.value:
            print( msg )
            self.write_logfile( msg )

        if err_class == ERR.WARN and self.verbosity >= err_class.value:
            print( msg, file=stderr )
            self.write_logfile( msg )

        # always show fatal errors anyway
        if err_class == ERR.FATAL:
            print( msg, file=stderr )
            self.write_logfile( msg )

        # if channel matches verbosity
        if err_class == ERR.DEBUG and self.verbosity >= err_class.value:
            print( msg )
            self.write_logfile( msg )

    # let the object be callable to use the log method
    def __call__( self, err_class, msg ):
        return self.log( err_class, msg )


# S3io
# -
# The S3 interaction class.
# Abstracts low level S3 and AWS interaction to a high level interface
class S3io:
    def __init__( self, config ):
        self.config = config

        self.slog = Logger( 'S3 Orchestrator', config )

        # Do NOT assume the environment provided is configured for the execution context.
        try:
            # Create a session object tied to an environment.
            # self.session = boto3.Session( profile_name=self.config.environment )
            self.session = boto3.Session( profile_name=self.config.environment )
            self.slog( ERR.DEBUG, "boto3 session initiated" )

        except exceptions.ProfileNotFound:
            raise ConfigMissingAWSCLIProfile(
                "Specified profile '{0}' is not in your AWS CLI configuration.  Available options are: {1}".format(
                    config.environment,
                    boto3.session.Session().available_profiles
                ),
                "User Configuration Error"
            )

        self.client = self.session.client('s3')
        self.slog( ERR.DEBUG, "s3 client initialized")

    # in case we feel like scanning more than one bucket
    def list_buckets( self ):
        response = self.client.list_buckets()

        buckets = []

        # TODO add'l error handling, KeyError
        for bucket in response['Buckets']:
            buckets.append( bucket['Name'] )

        return buckets

    def list_bucket_contents( self, bucket_name ):
        # Beware of permissions issues here.

        # This function requires s3:ListObjects permissions which appears to currently be broken in its association
        # with the ListObjects{,V2} operations.
        # Reference: https://aws.amazon.com/premiumsupport/knowledge-center/s3-access-denied-listobjects-sync/

        files = []

        # create a reusable Paginator
        paginator = self.client.get_paginator( 'list_objects_v2' )

        try:
            # create a PageIterator from the Paginator
            pages = paginator.paginate( Bucket=bucket_name )
            for page in pages:
                for item in page['Contents']:
                    files.append(item['Key'])
        except exceptions.ClientError:
            raise InvalidPermissions(
                "Your user doesn't have access to list objects in S3 bucket '{0}'".format( bucket_name ),
                "ListObjectsV2"
            )
        return files

    # downloads objects by name from the S3 bucket
    def download_file( self, remote_filename, local_filename ):
        self.slog(
            ERR.INFO,
            "Downloading '{0}' from S3 bucket '{1}' to '{2}'".format(
                remote_filename,
                self.config.bucket,
                local_filename
            )
        )

        try:
            # download the file, flatten any directories in case it already exists
            self.client.download_file( self.config.bucket, remote_filename, local_filename )
        except exceptions.ClientError:
            raise InvalidPermissions(
                "Your user doesn't have access to download '{0}' from the S3 bucket '{1}'".format(
                    remote_filename,
                    self.config.bucket
                ),
                "Permissions Issue"
            )

    # uploads files as S3 objects
    def upload_file( self, local_filename, object_name ):
        self.slog(
            ERR.INFO,
            "Uploading '{0}' to S3 bucket '{1}' with path '{2}'".format(
                local_filename,
                self.config.bucket,
                object_name
            )
        )
        self.client.upload_file( local_filename, self.config.bucket, object_name )

    # deletes S3 objects by name/path
    def delete_file( self, object_name ):
        self.slog(
            ERR.WARN,
            "Deleting file '{0}' from S3 bucket '{1}'".format(
                object_name,
                self.config.bucket
            )
        )
        self.client.delete_object( Bucket=self.config.bucket, Key=object_name )


# TrackLister
# used for the various methods for sortie to determine which tracks to process from the S3 bucket.  if set to use s3 as
# track list source you must supply a keyword argument of orchestrator to represent the s3io object
class TrackLister:
    # using a flexible constructor to account for s3 scanning
    def __init__( self, *args, **kwargs ):
        self.config = kwargs.get( 'config' )
        self.slog = Logger( 'TrackLister', self.config )

        self.mode = self.config.ingestion_mode
        self.slog(ERR.DEBUG, "Using track listing mode '{0}'".format( self.mode ) )

        if self.mode == 'track_list':
            self.list_file = self.config.track_list
            self.tracks = self.ingest_trackfile( self.list_file )

        elif self.mode == 'dynamic':
            self.tracks = self.ingest_s3( kwargs.get( 'orchestrator' ) )

        elif self.mode == 'cache':
            self.cache_dir = self.config.cache_dir
            self.tracks = self.ingest_cache( self.cache_dir )
        else:
            raise InvalidValue(
                "Please see the instructions and adjust your configuration file.",
                "Configuration Issue"
            )

        self.slog( ERR.INFO, "Tracks found: {0}".format( self.tracks ) )
        self.slog( ERR.DEBUG, "Track Ingestor Initialized" )

    # returns a list of files from a track list file
    def ingest_trackfile( self, filepath ):
        tracks = []
        with open( filepath, 'r' ) as json_file:
            data = json.load( json_file )
            for track in data['input']:
                if track.endswith( '.mp3' ):
                    tracks.append( track )
        return tracks

    # returns a list of tracks from a flat cache directory
    def ingest_cache( self, directory ):
        tracks = []
        for root, subs, files in walk( directory ):
            for file in files:
                if file.endswith( '.mp3' ):
                    tracks.append( file )
        return tracks

    # get list of tracks from the target S3 to process
    # requires permissions!
    def ingest_s3( self, orchestrator ):
        contents = orchestrator.list_bucket_contents( orchestrator.config.bucket )
        self.slog( ERR.DEBUG, "S3 contents found: {0}".format( contents ) )

        tracks = []
        for item in contents:
            if not item.endswith( '/' ) and item.endswith( '.mp3' ):
                tracks.append( item )
        return tracks


# turn ID3 tags into a target dest
class TrackConverter:
    def __init__( self, config, filepath ):
        self.slog = Logger( 'TrackConverter', config )

        self.local_path = filepath
        self.tags = self.load_tags( filepath )

        self.target_path = self.load_target_template( config.sort_mask )

        self.slog( ERR.DEBUG, "ID3/TEMPLATE conversion engine initialized")

    # grab all the ID3 tags from an MP3
    def load_tags( self, filepath ):
        tags = TinyTag.get( filepath )
        self.slog( ERR.DEBUG, "Tags found for '{0}': {1}".format(
            filepath,
            tags
        ))
        return tags

    # generate the target path in the S3 bucket according to the mask we used
    def load_target_template( self, ref ):
        tm = Template( ref )
        # TODO add some magic tag conversion to make the template useful
        target_path = tm.render(
            artist=self.tags.artist,
            album=self.tags.album,
            title=self.tags.title
        )
        self.slog( ERR.INFO, "Target path: {0}".format( target_path ) )
        return target_path


# download all tracks; helper for Main()
def download_all_tracks( config, client, tracks ):
    for track in tracks:
        # TODO duffing - clean this up
        client.download_file(
            track,
            "{0}/{1}.mp3".format(
                config.cache_dir,
                str(
                    uuid.uuid5(
                        uuid.NAMESPACE_OID,
                        track
                    )
                )
            )
        )


# upload a list of tracks to the S3 bucket
def upload_all_tracks( client, tracks ):
    for track in tracks:
        client.upload_file( track.local_path, track.target_path )


# delete a list of tracks in the S3 bucket; this assumes you dont want multiple copies of the same tracks after they're
# sorted and re-uploaded
def delete_source_tracks( client, tracks ):
    for track in tracks:
        client.delete_file( track )


# helper to isolate cache slurping
def slurp_cache( slog, config ):
    # workaround object model flaw
    cacheconf = config
    cacheconf.ingestion_mode = 'cache'

    cache_lister = TrackLister( config=cacheconf )
    cache_list = cache_lister.ingest_cache( cacheconf.cache_dir )
    sorted_tracks = []
    for track in cache_list:
        slog(ERR.INFO, "Cache Found: {0}".format( track ))
        converted = TrackConverter( config, "{0}/{1}".format( cacheconf.cache_dir, track ) )
        sorted_tracks.append( converted )
    return sorted_tracks

# entry point
def Main():
    # get the current directory
    current_dir = path.dirname( path.abspath(  __file__  ) )

    # initialize config
    # create a config object pointed at a locally referenced ini file.
    config = Config( "{0}/conf/sortie.ini".format( current_dir ) )

    # initialize logger for main
    slog = Logger( 'sortie', config )

    # show in the log we're starting a new run
    slog( ERR.INFO, "new run started" )

    # initialize the S3 orchestrator
    client = S3io( config )

    # initialize the track source lister
    if config.ingestion_mode == 'dynamic':
        # dynamic track ingestion from an s3 source requires an s3io object
        source_lister = TrackLister( config=config, orchestrator=client )
    else:
        slog( ERR.INFO, "Ingestion mode is '{0}'".format( config.ingestion_mode ) )
        # all other ingestion methods don't
        source_lister = TrackLister( config=config )

    if not config.ingestion_mode == 'cache':
        # populate cache
        download_all_tracks( config, client, source_lister.tracks )

    # get a list of tracks in LOCAL CACHE
    cache_list = slurp_cache( slog, config )

    # upload all the TrackConverter objects according to their properties
    upload_all_tracks( client, cache_list )

    # did the user set the option to delete tracks after they're sorted and uploaded?
    if config.clean_up and not config.ingestion_mode == 'cache':
        delete_source_tracks( client, source_lister.tracks )

    # did the user set the option to persist cache between runs?
    if not config.persistent_cache:
        slog( ERR.WARN, "erasing cache directory '{0}'".format( config.cache_dir ) )
        shutil.rmtree( config.cache_dir, ignore_errors=False, onerror=None )

    slog( ERR.INFO, "run completed" )


# declare the entrypoint explicitly since this is a utility
if __name__=='__main__':
    Main()
