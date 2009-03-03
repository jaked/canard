module Q = Persistent_queue

type journal_item = [
| `Add of Q.item
| `Remove
| `Remove_tentative
| `Saved_xid of int32
| `Unremove of int32
| `Confirm_remove of int32
| `End_of_file
]

type t = {
  queue_path : string;

  mutable writer : Netchannels.out_obj_channel;
  mutable reader : Netchannels.in_obj_channel option;
  mutable replayer : Netchannels.in_obj_channel option;

  mutable size : int64;

  buffer : Buffer.t;
}

let cmd_add = Char.chr 0
let cmd_remove = Char.chr 1
let cmd_addx = Char.chr 2
let cmd_remove_tentative = Char.chr 3
let cmd_save_xid = Char.chr 4
let cmd_unremove = Char.chr 5
let cmd_confirm_remove = Char.chr 6
let cmd_add_xid = Char.chr 7

let open_file path =
  Netchannels.lift_out ~buffered:false
    (`Raw (new Netchannels.output_descr (Unix.openfile path [ Unix.O_WRONLY; Unix.O_APPEND ] 0o600)))

let size_add t i =
  t.size <- Int64.add t.size (Int64.of_int i)

let now () = Int64.of_float (Unix.gettimeofday () *. 1000.)

let rec make queue_path = {
  queue_path = queue_path;
  writer = open_file queue_path;
  reader = None;
  replayer = None;
  size = 0L;
  buffer = Buffer.create 16;
}

and roll t xid open_items queue =
  t.writer#close_out ();
  let backup_file = t.queue_path ^ "." ^ Int64.to_string (now ()) in
  Unix.rename t.queue_path backup_file;
  t.writer <- open_file t.queue_path;
  t.size <- 0L;
  ListLabels.iter open_items ~f:(fun item ->
    add_with_xid t item;
    remove_tentative t);
  save_xid t xid;
  Queue.iter (add t) queue;
  Unix.unlink backup_file

and close t =
  t.writer#close_out ();
  (match t.reader with Some c -> c#close_in () | _ -> ());
  t.reader <- None

and erase t =
  try
    close t;
    Unix.unlink t.queue_path
  with _ -> ()

and in_read_behind t = t.reader <> None

and add t item =
  let blob = pack item in
  let len = String.length blob in
  size_add t (write t [`Char cmd_addx; `Int len]);
  t.writer#output_string blob;
  size_add t len

and add_with_xid t item =
  let blob = pack item in
  let len = String.length blob in
  size_add t (write t [`Char cmd_add_xid; `Int32 item.Q.xid; `Int len]);
  t.writer#output_string blob;
  size_add t len

and remove t =
  size_add t (write t [`Char cmd_remove])

and remove_tentative t =
  size_add t (write t [`Char cmd_remove_tentative])

and save_xid t xid =
  size_add t (write t [`Char cmd_save_xid; `Int32 xid])

and unremove t xid =
  size_add t (write t [`Char cmd_unremove; `Int32 xid])

and confirm_remove t xid =
  size_add t (write t [`Char cmd_confirm_remove; `Int32 xid])

and start_read_behind t =
  let pos = match t.replayer with Some r -> r#pos_in | None -> t.writer#pos_out in
  let fd = Unix.openfile t.queue_path [ Unix.O_RDONLY ] 0o600 in
  ignore (Unix.lseek fd pos Unix.SEEK_SET);
  let rj = Netchannels.lift_in ~buffered:false (`Raw (new Netchannels.input_descr fd)) in
  t.reader <- Some rj

and fill_read_behind t f =
  let pos = match t.replayer with Some r -> r#pos_in | None -> t.writer#pos_out in
  match t.reader with
    | None -> ()
    | Some rj ->
        if rj#pos_in = pos
        then (rj#close_in (); t.reader <- None)
        else
          match read_journal_entry t rj false with
            | `Add item -> f item
            | _ -> ()

and replay t name f =
  t.size <- 0L;
  begin
    try
      let inch =
        Netchannels.lift_in ~buffered:false
          (`Raw (new Netchannels.input_descr (Unix.openfile t.queue_path [ Unix.O_RDONLY ] 0o600))) in
      t.replayer <- Some inch;
      let rec loop () =
      match read_journal_entry t inch true with
        | `End_of_file -> ()
        | item -> f item; loop () in
      loop ()
    with
      | Unix.Unix_error (Unix.ENOENT, _, _) ->
          Netplex_cenv.logf `Info "No transaction journal for '%s'; starting with empty queue." name
      | Unix.Unix_error _ as e ->
          Netplex_cenv.logf `Err "Exception %s replaying journal for '%s'" (Printexc.to_string e) name
  end;
  t.replayer <- None

and read_journal_entry t inch replaying =
  try
    match inch#input_char () with
      | b when b = cmd_add ->
          let data = read_block t inch in
          if replaying then size_add t (5 + String.length data);
          `Add (unpack_old_add data)
      | b when b = cmd_remove ->
          if replaying then size_add t 1;
          `Remove
      | b when b = cmd_addx ->
          let data = read_block t inch in
          if replaying then size_add t (5 + String.length data);
          `Add (unpack data)
      | b when b = cmd_remove_tentative ->
          if replaying then size_add t 1;
          `Remove_tentative
      | b when b = cmd_save_xid ->
          let xid = read_int32 t inch in
          if replaying then size_add t 5;
          `Saved_xid xid
      | b when b = cmd_unremove ->
          let xid = read_int32 t inch in
          if replaying then size_add t 5;
          `Unremove xid
      | b when b = cmd_confirm_remove ->
          let xid = read_int32 t inch in
          if replaying then size_add t 5;
          `Confirm_remove xid
      | b when b = cmd_add_xid ->
          let xid = read_int32 t inch in
          let data = read_block t inch in
          let item = unpack ~xid data in
          if replaying then size_add t (9 + String.length data);
          `Add item
      | b ->
          raise (Failure ("invalid opcode in journal: " ^ string_of_int (Char.code b)))
  with End_of_file -> `End_of_file

and read_block t inch =
  let size = Int32.to_int (read_int32 t inch) in
  let data = String.create size in
  inch#really_input data 0 size;
  data

and read_int32 t inch =
  Buffer.clear t.buffer;
  for i=0 to 3 do
    Buffer.add_char t.buffer (inch#input_char ())
  done;
  Bits.get_int32 (Buffer.contents t.buffer) 0

and write t items =
  Buffer.clear t.buffer;
  ListLabels.iter items ~f:(function
    | `Char c -> Buffer.add_char t.buffer c
    | `Int i -> Bits.put_int32 t.buffer (Int32.of_int i)
    | `Int32 i -> Bits.put_int32 t.buffer i);
  t.writer#output_buffer t.buffer;
  Buffer.length t.buffer

and pack item =
  let buf = Buffer.create (String.length item.Q.data + 16) in
  Bits.put_int64 buf item.Q.add_time;
  Bits.put_int64 buf item.Q.expiry;
  Buffer.add_string buf item.Q.data;
  Buffer.contents buf

and unpack ?(xid=Int32.zero) data =
  let bytes = String.sub data 16 (String.length data - 16) in
  let add_time = Bits.get_int64 data 0 in
  let expiry = Bits.get_int64 data 8 in
  { Q.add_time = add_time; expiry = expiry; data = bytes; xid = xid }

and unpack_old_add data =
  let bytes = String.sub data 4 (String.length data - 4) in
  let expiry = Int64.of_int32 (Bits.get_int32 data 0) in
  { Q.add_time = now (); expiry = Int64.mul expiry 1000L; data = bytes; xid = Int32.zero }
