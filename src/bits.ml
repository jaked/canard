(* adapted from Ocamlnet's Rtypes *)

let c_0xff_32 = Int32.of_string "0xff" ;;
let c_0xff_64 = Int64.of_string "0xff" ;;

let put_int32 b x =
  Buffer.add_char b (Char.chr (Int32.to_int (Int32.logand x c_0xff_32)));
  Buffer.add_char b (Char.chr (Int32.to_int (Int32.shift_right_logical x 8) land 0xff));
  Buffer.add_char b (Char.chr (Int32.to_int (Int32.shift_right_logical x 16) land 0xff));
  Buffer.add_char b (Char.chr (Int32.to_int (Int32.shift_right_logical x 24) land 0xff))

let put_int64 b x =
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand x c_0xff_64)));
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical x 8) c_0xff_64)));
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical x 16) c_0xff_64)));
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical x 24) c_0xff_64)));
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical x 32) c_0xff_64)));
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical x 40) c_0xff_64)));
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical x 48) c_0xff_64)));
  Buffer.add_char b (Char.chr (Int64.to_int (Int64.logand (Int64.shift_right_logical x 56) c_0xff_64)))

let get_int32 s pos =
  let n3 = Int32.of_int (Char.code s.[pos+3]) in
  let x = Int32.shift_left n3 24 in
  let n2 = Int32.of_int (Char.code s.[pos+2]) in
  let x = Int32.logor x (Int32.shift_left n2 16) in
  let n1 = Int32.of_int (Char.code s.[pos+1]) in
  let x = Int32.logor x (Int32.shift_left n1 8) in
  let n0 = Int32.of_int (Char.code (s.[pos])) in
  Int32.logor x n0

let get_int64 s pos =
  let n7 = Int64.of_int (Char.code s.[pos+7]) in
  let x = Int64.shift_left n7 56 in
  let n6 = Int64.of_int (Char.code s.[pos+6]) in
  let x = Int64.logor x (Int64.shift_left n6 48) in
  let n5 = Int64.of_int (Char.code s.[pos+5]) in
  let x = Int64.logor x (Int64.shift_left n5 40) in
  let n4 = Int64.of_int (Char.code s.[pos+4]) in
  let x = Int64.logor x (Int64.shift_left n4 32) in
  let n3 = Int64.of_int (Char.code s.[pos+3]) in
  let x = Int64.logor x (Int64.shift_left n3 24) in
  let n2 = Int64.of_int (Char.code s.[pos+2]) in
  let x = Int64.logor x (Int64.shift_left n2 16) in
  let n1 = Int64.of_int (Char.code s.[pos+1]) in
  let x = Int64.logor x (Int64.shift_left n1 8) in
  let n0 = Int64.of_int (Char.code s.[pos]) in
  Int64.logor x n0
