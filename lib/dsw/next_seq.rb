class NextSeq < Sequencer

  def seq_complete?
    File.exists?(File.join(run_dir, 'RunCompletionStatus.xml'))
  end

end
