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
