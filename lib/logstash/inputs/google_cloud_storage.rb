# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::GoogleCloudStorage < LogStash::Inputs::Base
  config_name "google_cloud_storage"
  default :codec, "plain"

  config :project

  # Path to JSON file containing the Service Account credentials (not needed when running inside GCE)
  config :keyfile

  # GCS private key password.
  config :key_password, :validate => :string, :default => "notasecret"
  #GCS service account
  config :service_account, :validate => :string, :required => true

  # The name of the GCS bucket.
  config :bucket, :validate => :string, :required => true

  # If specified, the prefix of filenames in the bucket must match (not a regexp)
  config :prefix, :validate => :string, :default => nil

  # Where to write the since database (keeps track of the date
  # the last handled file was added to S3). The default will write
  # sincedb files to some path matching "$HOME/.sincedb*"
  # Should be a path with filename not just a directory.
  config :sincedb_path, :validate => :string, :default => nil

  # Name of a S3 bucket to backup processed files to.
  config :backup_to_bucket, :validate => :string, :default => nil

  # Append a prefix to the key (full path including file name in s3) after processing.
  # If backing up to another (or the same) bucket, this effectively lets you
  # choose a new 'folder' to place the files in
  config :backup_add_prefix, :validate => :string, :default => nil

  # Path of a local directory to backup processed files to.
  config :backup_to_dir, :validate => :string, :default => nil

  # Whether to delete processed files from the original bucket.
  config :delete, :validate => :boolean, :default => false

  # Interval to wait between to check the file list again after a run is finished.
  # Value is in seconds.
  config :interval, :validate => :number, :default => 60

  # Ruby style regexp of keys to exclude from the bucket
  config :exclude_pattern, :validate => :string, :default => nil

  # Set the directory where logstash will store the tmp files before processing them.
  # default to the current OS temporary directory in linux /tmp/logstash
  config :temporary_directory, :validate => :string, :default => File.join(Dir.tmpdir, "logstash")


  public
  def register
    @host = Socket.gethostname

    require "fileutils"
    require "digest/md5"
    require "google/api_client"
    require "openssl"

    @logger.info("Registering GCS input", :bucket => @bucket, :project => @project, :keyfile => @keyfile)
    # initialize_google_client

    @client = Google::APIClient.new(:application_name =>
                                        'Logstash Google Cloud Storage input plugin',
                                    :application_version => '0.1')

    key = Google::APIClient::PKCS12.load_key(@keyfile, @key_password)
    service_account = Google::APIClient::JWTAsserter.new(@service_account,
                                                         'https://www.googleapis.com/auth/devstorage.read_write',
                                                         key)
    @client.authorization = service_account.authorize
    @gcs = @client.discovered_api('storage', 'v1')


    #list_new_files


    unless @backup_to_dir.nil?
      Dir.mkdir(@backup_to_dir, 0700) unless File.exists?(@backup_to_dir)
    end

    FileUtils.mkdir_p(@temporary_directory) unless Dir.exist?(@temporary_directory)
  end # def register

  def run(queue)
    @current_thread = Thread.current
    Stud.interval(@interval) do
      process_files(queue)
    end
  end # def run



  public
  def list_new_files
    @logger.debug("GCS input: Polling")

    objects = @client.execute(
        api_method: @gcs.objects.list,
        parameters: {bucket: @bucket}
    )

  #  @logger.info(objects.body)

    logFiles = {}

    objects.data.items.each do |file|

      unless ignore_filename?(file.name)
        if sincedb.newer?(file.updated)
          logFiles[file.name] = file.updated
          @logger.debug("GCS input: Adding to objects[]", :name => file.name)
          @logger.debug("objects[] length is: ", :length => logFiles.length)
        end
      end

    end


    return logFiles.keys.sort {|a,b| logFiles[a] <=> logFiles[b]}
  end # def fetch_new_files




  public
  def backup_to_bucket(filename)
    unless @backup_to_bucket.nil?
      backup_key = "#{@backup_add_prefix}#{filename}"

      @logger.info ("bck_up object " + backup_key)

      result = @client.execute(
          api_method: @gcs.objects.copy,
          parameters: {destinationBucket: @backup_to_bucket, destinationObject:backup_key, sourceBucket: @bucket, sourceObject: filename }
      )

      if @delete
        result = @client.execute(
            api_method: @gcs.objects.delete,
            parameters: { bucket: @bucket, object: filename }
        )
      end
    end
  end

  public
  def backup_to_dir(filename)
    unless @backup_to_dir.nil?
      FileUtils.cp(filename, @backup_to_dir)
    end
  end


  public
  def process_files(queue)
    objects = list_new_files

    objects.each do |filename|
      if stop?
        break
      else
        @logger.info("gcs input processing", :bucket => @bucket, :filename => filename)
        process_log(queue, filename)
      end
    end
  end # def process_files



  public
  def stop
    # @current_thread is initialized in the `#run` method,
    # this variable is needed because the `#stop` is a called in another thread
    # than the `#run` method and requiring us to call stop! with a explicit thread.
    Stud.stop!(@current_thread)
  end


  private

  # Read the content of the local file
  #
  # @param [Queue] Where to push the event
  # @param [String] Which file to read from
  # @return [Boolean] True if the file was completely read, false otherwise.
  def process_local_log(queue, filename, key)
    @logger.info('Processing file', :filename => filename)
    metadata = {}
    # Currently codecs operates on bytes instead of stream.
    # So all IO stuff: decompression, reading need to be done in the actual
    # input and send as bytes to the codecs.
    read_file(filename) do |line|
      if stop?
        @logger.warn("Logstash GCS input, stop reading in the middle of the file, we will read it again when logstash is started")
        return false
      end

      @codec.decode(line) do |event|
        # We are making an assumption concerning cloudfront
        # log format, the user will use the plain or the line codec
        # and the message key will represent the actual line content.
        # If the event is only metadata the event will be drop.
        # This was the behavior of the pre 1.5 plugin.
        #
        # The line need to go through the codecs to replace
        # unknown bytes in the log stream before doing a regexp match or
        # you will get a `Error: invalid byte sequence in UTF-8'
        if event_is_metadata?(event)
          @logger.debug('Event is metadata, updating the current cloudfront metadata', :event => event)
          update_metadata(metadata, event)
        else
          decorate(event)

          event.set("cloudfront_version", metadata[:cloudfront_version]) unless metadata[:cloudfront_version].nil?
          event.set("cloudfront_fields", metadata[:cloudfront_fields]) unless metadata[:cloudfront_fields].nil?

          event.set("[@metadata][s3]", { "key" => key })

          queue << event
        end
      end
    end

    return true
  end # def process_local_log


  private
  def event_is_metadata?(event)
    return false unless event.get("message").class == String
    line = event.get("message")
    version_metadata?(line) || fields_metadata?(line)
  end

  private
  def version_metadata?(line)
    line.start_with?('#Version: ')
  end

  private
  def fields_metadata?(line)
    line.start_with?('#Fields: ')
  end

  private
  def update_metadata(metadata, event)
    line = event.get('message').strip

    if version_metadata?(line)
      metadata[:cloudfront_version] = line.split(/#Version: (.+)/).last
    end

    if fields_metadata?(line)
      metadata[:cloudfront_fields] = line.split(/#Fields: (.+)/).last
    end
  end

  private
  def read_file(filename, &block)
    if gzip?(filename)
      read_gzip_file(filename, block)
    else
      read_plain_file(filename, block)
    end
  end

  def read_plain_file(filename, block)
    File.open(filename, 'rb') do |file|
      file.each(&block)
    end
  end

  private
  def read_gzip_file(filename, block)
    # Details about multiple streams and the usage of unused from: http://code.activestate.com/lists/ruby-talk/11168/
    File.open(filename) do |zio|
      while true do
        io = Zlib::GzipReader.new(zio)
        io.each_line { |line| block.call(line) }
        unused = io.unused
        io.finish
        break if unused.nil?
        zio.pos -= unused.length # reset the position to the other block in the stream
      end
    end
  rescue Zlib::Error, Zlib::GzipFile::Error => e
    @logger.error("Gzip codec: We cannot uncompress the gzip file", :filename => filename)
    raise e
  end

  private
  def gzip?(filename)
    filename.end_with?('.gz')
  end

  private
  def sincedb
    @sincedb ||= if @sincedb_path.nil?
                   @logger.info("Using default generated file for the sincedb", :filename => sincedb_file)
                   SinceDB::File.new(sincedb_file)
                 else
                   @logger.info("Using the provided sincedb_path",
                                :sincedb_path => @sincedb_path)
                   SinceDB::File.new(@sincedb_path)
                 end
  end

  private
  def sincedb_file
    File.join(ENV["HOME"], ".sincedb_" + Digest::MD5.hexdigest("#{@bucket}+#{@prefix}"))
  end

  private
  def ignore_filename?(filename)
    if @prefix == filename
      return true
    elsif filename.end_with?("/")
      return true
    elsif (@backup_add_prefix && @backup_to_bucket == @bucket && filename =~ /^#{backup_add_prefix}/)
      return true
    elsif @exclude_pattern.nil?
      return false
    elsif filename =~ Regexp.new(@exclude_pattern)
      return true
    else
      return false
    end
  end

  private
  def process_log(queue, key)

    @logger.info("going to get and download file "+ key)

    object = @client.execute(
        api_method: @gcs.objects.get,
        parameters: {bucket: @bucket, object: key}
    )

    objectdata = @client.execute(
        api_method: @gcs.objects.get,
        parameters: {bucket: @bucket, object: key, alt: 'media'}
    )

    # @logger.info(" after read " + object.body)


    filename = File.join(temporary_directory, File.basename(key))
    @logger.info(" generated filename " + filename)

    if download_remote_file(objectdata.body, filename)
      @logger.info(" downloaded")
      if process_local_log(queue, filename, key)
        @logger.info(" UPDATED " + object.data['updated'].to_s )
        lastmod = object.data['updated']
        backup_to_bucket(key)
        backup_to_dir(filename)
        delete_file_from_bucket(key)
        FileUtils.remove_entry_secure(filename, true)
        sincedb.write(lastmod)

      end
    else
      FileUtils.remove_entry_secure(filename, true)
    end
  end

  private
  # Stream the remove file to the local disk
  #
  # @param [S3Object] Reference to the remove S3 objec to download
  # @param [String] The Temporary filename to stream to.
  # @return [Boolean] True if the file was completely downloaded
  def download_remote_file(remote_object, local_filename)
    completed = false
    #@logger.info("gcs input: Download remote file", :remote_key => remote_object.name, :local_filename => local_filename)

    ##TODO NEED CHANGE IN WRITE LOGIC TO AVOID MEMORY ISSUES

    File.open(local_filename, 'wb') { |file| file.write(remote_object) }
    completed = true

    return completed
  end

  private
  def delete_file_from_bucket(object)
    if @delete and @backup_to_bucket.nil?
      object.delete()
    end
  end

  private
  def get_s3object
    s3 = Aws::S3::Resource.new(aws_options_hash)
  end

  private
  module SinceDB
    class File
      def initialize(file)
        @sincedb_path = file
      end

      def newer?(date)
        date > read
      end

      def read
        if ::File.exists?(@sincedb_path)
          content = ::File.read(@sincedb_path).chomp.strip
          # If the file was created but we didn't have the time to write to it
          return content.empty? ? Time.new(0) : Time.parse(content)
        else
          return Time.new(0)
        end
      end

      def write(since = nil)
        since = Time.now() if since.nil?
        ::File.open(@sincedb_path, 'w') { |file| file.write(since.to_s) }
      end
    end
  end
end # class LogStash::Inputs::GoogleCloudStorage


###  my other snippets :: jaspaul ::
# # List all buckets in the project
# bucket_list_result = @client.execute(
#     api_method: @gcs.buckets.list,
#     parameters: {project: @project}
# )
#
# @logger.info(" buckets body: " + bucket_list_result.body)
