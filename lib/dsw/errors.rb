module Errors
  class FindProcessError < StandardError; end
  class RsyncProcessError < StandardError; end
  class DuplicityProcessError < StandardError; end
  class DuplicityLockError < StandardError; end
  class DemultiplexProcessError < StandardError; end
  class DemultiplexLockError < StandardError; end
  class DuplicateRunError < StandardError; end
  class SkipException < StandardError; end
  class SequencingError < StandardError; end
  class UnknownMachineTypeError < StandardError; end
  class EmptyRunDirPathError < StandardError; end
  class UnknownMapFile < StandardError; end
end
