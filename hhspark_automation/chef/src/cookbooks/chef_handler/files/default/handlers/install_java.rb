class InstallJava
  def ShSource(filename)
  # Inspired by user takeccho at http://stackoverflow.com/a/26381374/3849157
  # Sources sh-script or env file and imports resulting environment
        fail(ArgumentError,"File #{filename} invalid or doesn't exist.") \
        unless File.exist?(filename)

        value = %x(./#{filename})
        puts "completed java installation"
  end
end

