require 'rspec'
require_relative 'lib/workflow.rb'

$root = "/tmp/dsw-test"
Conf.global_conf[:basecall_dir] = $root

describe Sequencer do
  it 'selects the appropriate subclass from the sequencer directory' do
    expect(Sequencer.select("#{$root}/.seq_MN00157/this-is-a/run-directory")).
      to be_instance_of(MiniSeq)
    expect(Sequencer.select("#{$root}/.seq_NB501326/this-is-alsa-a/run-directory")).
      to be_instance_of(NextSeq)
    expect(Sequencer.select("#{$root}/.seq_NS500455/this-is-alsa-a/run-directory")).
      to be_instance_of(NextSeq)
    expect(Sequencer.select("#{$root}/.seq_NS500648/this-is-alsa-a/run-directory")).
      to be_instance_of(NextSeq)
    expect(Sequencer.select("#{$root}/.seq_SN541/this-is-alsa-a/run-directory")).
      to be_instance_of(HiSeq)
    expect(Sequencer.select("#{$root}/.seq_K00302/run-dir")).
      to be_instance_of(HiSeq)

    expect{Sequencer.select("#{$root}/.seq_ISEQ100/run-dir")}.
      to raise_error(Errors::UnknownMachineTypeError)
  end

  it 'selects the appropriate subclass from the run directory' do
    expect(Sequencer.select("#{$root}/200109_NB501326_0356_AHTWG2BGXC/")).
      to be_instance_of(NextSeq)
  end
end

describe NextSeq do
  before(:each) {
    @dir = "#{$root}/.seq_NB501326"
    FileUtils.remove_dir @dir
    FileUtils.mkdir_p @dir
  }

  it 'assumes the sequencer is done when RTAComplete.txt is at least 15 mins old' do
    FileUtils.mkdir_p "#{@dir}/my-run-dir"

    seq = Sequencer.select("#{@dir}/my-run-dir")
    expect(seq.seq_complete?).to be false

    FileUtils.touch "#{@dir}/my-run-dir/RTAComplete.txt"
    expect(seq.seq_complete?).to be false

    FileUtils.touch "#{@dir}/my-run-dir/RTAComplete.txt",
                    :mtime => Time.now - (14 * 60)
    expect(seq.seq_complete?).to be false

    FileUtils.touch "#{@dir}/my-run-dir/RTAComplete.txt",
                    :mtime => Time.now - (15 * 60)
    expect(seq.seq_complete?).to be true
  end

  it 'assumes the sequencer is done when RunCompletionStatus.xml is at least 15 mins old' do
    FileUtils.mkdir_p "#{@dir}/my-run-dir"

    seq = Sequencer.select("#{@dir}/my-run-dir")
    expect(seq.seq_complete?).to be false

    FileUtils.touch "#{@dir}/my-run-dir/RunCompletionStatus.xml"
    expect(seq.seq_complete?).to be false

    FileUtils.touch "#{@dir}/my-run-dir/RunCompletionStatus.xml",
                    :mtime => Time.now - (14 * 60)
    expect(seq.seq_complete?).to be false

    FileUtils.touch "#{@dir}/my-run-dir/RunCompletionStatus.xml",
                    :mtime => Time.now - (15 * 60)
    expect(seq.seq_complete?).to be true
  end
end
