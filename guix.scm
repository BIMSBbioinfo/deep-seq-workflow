;;; dsw - Deep Seq Workflow
;;; Copyright © 2017, 2020 Ricardo Wurmus <rekado@elephly.net>
;;;
;;; This file is part of dsw.
;;;
;;; dsw is free software; see LICENSE file for details.
;;;
;;; Run the following command to enter a development environment for
;;; dsw:
;;;
;;;  $ guix environment -l guix.scm
;;;

(use-modules (guix packages)
             (guix licenses)
             (guix build-system ruby)
             (gnu packages)
             (gnu packages ruby))

(package
  (name "dsw")
  (version "0.0.0")
  (source #f)
  (build-system ruby-build-system)
  (arguments `(#:tests? #f))  ; there are none
  (inputs
   `(("ruby-rsync" ,ruby-rsync)
     ("ruby-childprocess" ,ruby-childprocess)
     ("ruby-commander" ,ruby-commander)))
  (native-inputs
   `(("ruby-rspec" ,ruby-rspec)))
  (home-page "https://github.com/BIMSBbioinfo/deep-seq-workflow")
  (synopsis "TODO")
  (description "TODO")
  (license expat))
