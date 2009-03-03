let start () =
  let (opt_list, cmdline_cfg) = Netplex_main.args () in

  Arg.parse
    opt_list
    (fun s -> raise (Arg.Bad ("Don't know what to do with: " ^ s)))
    "usage: canard [options]";

  let factories = [
    let hooks =
      object
        inherit Netplex_kit.empty_processor_hooks () as super

        method post_start_hook c =
          Lwt_equeue.set_event_system c#event_system

        method shutdown () =
          Lwt_equeue.unset_event_system ()
      end in

    object
      method name = "canard"
      method create_processor _ _ _ =
        object (self)
          inherit Netplex_kit.processor_base hooks

          method process ~when_done container fd _ =
            Lwt.ignore_result
              (Lwt.finalize
                  (fun () ->
                    Lwt.catch
                      (fun () -> Handler.handler fd)
                      (fun e -> ignore (self#global_exception_handler e); Lwt.return ()))
                  (fun () -> when_done (); Lwt.return ()))

          method supported_ptypes = [ `Multi_processing ]
        end
    end
  ] in

  Netplex_main.startup
    (Netplex_mp.mp ())
    Netplex_log.logger_factories
    Netplex_workload.workload_manager_factories
    factories
    cmdline_cfg
;;

Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
start()
