exception Protocol_error of string

let (>>=) = Lwt.(>>=)

let handler fd =
  let descr = Lwt_unix.of_unix_file_descr fd in
  let inch = Lwt_chan.in_channel_of_descr descr in
  let ouch = Lwt_chan.out_channel_of_descr descr in

  let pending_txn = ref None in

  let decode () =
    let bad_req () = raise (Protocol_error "Malformed request line") in

    Lwt_chan.input_line inch >>= fun line ->
      match ExtString.String.nsplit line " " with
        | [] -> assert false
        | cmd :: args ->
            match String.uppercase cmd, args with
              | "GET", [ key ] ->
                  let res =
                    match ExtString.String.nsplit key "/" with
                      | [] -> assert false
                      | "" :: _ -> bad_req ()
                      | [ key ] -> `Get (key, 0, false, false)
                      | key :: opts ->
                          let timeout =
                            try
                              let t = List.find (fun o -> ExtString.String.exists o "t=") opts in
                              int_of_string (ExtString.String.slice ~first:2 t)
                            with _ -> 0 in
                          let closing = List.mem "close" opts in
                          let opening = List.mem "open" opts in
                          `Get (key, timeout, closing, opening) in
                  Lwt.return res

              | "SET", [ key; flags; expiry; size ] ->
                  let to_int s =
                    try int_of_string size
                    with _ -> bad_req () in
                  let flags, expiry, size = to_int flags, to_int expiry, to_int size in
                  let data = String.create (size + 2) in
                  Lwt_chan.really_input inch data 0 size >>= (fun () ->
                    let data = String.sub data 0 size in
                    Lwt.return (`Set (key, flags, expiry, data)))

              | "STATS", [] -> Lwt.return `Stats
              | "SHUTDOWN", [] -> Lwt.return `Shutdown
              | "RELOAD", [] -> Lwt.return `Reload
              | "FLUSH", [ key ] -> Lwt.return (`Flush key)
              | "DUMP_CONFIG", [] -> Lwt.return `Dump_config

              | _ -> bad_req () in

(*
  let handle = function
    | `Get (key, timeout, closing, opening) ->
*)

  Lwt.return ()
