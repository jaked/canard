type journal_item = [
| `Add of Persistent_queue.item
| `Confirm_remove of int32
| `End_of_file
| `Remove
| `Remove_tentative
| `Saved_xid of int32
| `Unremove of int32
]

type t

val make : string -> t
val roll : t -> int32 -> Persistent_queue.item list -> Persistent_queue.item Queue.t -> unit
val close : t -> unit
val erase : t -> unit
val in_read_behind : t -> bool
val add : t -> Persistent_queue.item -> unit
val remove : t -> unit
val remove_tentative : t -> unit
val unremove : t -> int32 -> unit
val confirm_remove : t -> int32 -> unit
val start_read_behind : t -> unit
val fill_read_behind : t -> (Persistent_queue.item -> unit) -> unit
val replay : t -> string -> (journal_item -> unit) -> unit
