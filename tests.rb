require 'rspec'
require_relative 'lib/workflow.rb'

$root = "/tmp/dsw-test"
Conf.global_conf[:basecall_dir] = $root

describe Sequencer do
  it 'selects the appropriate subclass from the sequencer directory' do
    expect(Sequencer.select("#{$root}/.seq_MN00157/this-is-a/run-directory")).
      to be_instance_of(MiniSeq)
  end

  it 'selects the appropriate subclass from the run directory' do
    expect(Sequencer.select("#{$root}/200109_NB501326_0356_AHTWG2BGXC/")).
      to be_instance_of(NextSeq)
  end
end
