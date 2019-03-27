(** Types and module types used in Elastic.

This module is included by elastic.ml\{,i\}.
*)

open Devkit

(** Host selection *)
module type Hosts = sig
  type state

  val new_request : unit -> state

  val random_host : state -> string
end

module type Host_array = sig
  val hosts : string array
end

(** Type of an action. Use [`Create] when you want to ensure that you
    create a document, i.e. not overwriting an existing document. *)
type 'doc any_action =
  [`Index of 'doc | `Create of 'doc | `Update of 'doc | `Delete]

type action = string any_action

type version =
  [`Auto | `Internal of int | `External_gt of int | `External_gte of int]

type 'a bulk_action = {action: action; meta: Elastic_j.index; tag: 'a}

type action_key_mode = Full | IgnoreIndexName

(** Args are (index, doc_type, id, routing, action). *)
type command = string * string * string option * string option * action

type command_full =
  string * string * string option * string option * version * action

type get_host = unit -> string

type ('tag, 'ret) bulk_write_loop =
     (int option * 'tag bulk_action) Enum.t
  -> (int option * 'tag bulk_action) Enum.t
  -> (   ?f_fail:(string -> int -> unit)
      -> ?f_fail_all:(string -> int -> unit)
      -> (int option * 'tag bulk_action) Enum.t
      -> 'ret)
  -> 'ret

module type Query = sig
  type 'a t

  (* Exceptions returned on errors: (ES HTTP return code, human-readable reason) *)

  exception ESError of int * string

  val aliases :
    ?retries:int -> ?timeout:int -> ?index:string -> unit -> string option t

  val docs_stats : ?retries:int -> ?timeout:int -> string -> string option t

  val refresh : ?retries:int -> ?timeout:int -> string list -> unit t

  val search :
       ?pdebug:(string -> unit)
    -> index:string
    -> kind:string
    -> ?verbose:bool
    -> ?should_exit:(unit -> unit t)
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> Elastic_query_dsl.top_query
    -> string option t

  val search_kinds :
       index:string
    -> ?kinds:string list
    -> ?verbose:bool
    -> ?should_exit:(unit -> unit t)
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> Elastic_query_dsl.top_query
    -> string option t

  val search_shards :
       index:string
    -> ?verbose:bool
    -> ?should_exit:(unit -> unit t)
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> unit
    -> Elastic_j.search_shards option t

  val put_string :
       ?should_exit:(unit -> unit t)
    -> ?version_conflict_handler:(   string
                                  -> string
                                  -> int
                                  -> string
                                  -> string option t)
    -> index:string
    -> kind:string
    -> ?id:string
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> string
    -> string option t

  val put :
       ?should_exit:(unit -> unit t)
    -> index:string
    -> kind:string
    -> ?id:string
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> Yojson.Basic.t
    -> Elastic_j.index_result option t

  val put_versioned :
       ?should_exit:(unit -> unit t)
    -> index:string
    -> kind:string
    -> id:string
    -> version:int
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> Yojson.Basic.t
    -> bool t

  val delete :
       ?should_exit:(unit -> unit t)
    -> index:string
    -> kind:string
    -> id:string
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> unit
    -> unit t

  val update :
       ?should_exit:(unit -> unit t)
    -> ?upsert:bool
    -> index:string
    -> kind:string
    -> id:string
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> Yojson.Basic.t
    -> unit t

  val update_string :
       ?should_exit:(unit -> unit t)
    -> index:string
    -> kind:string
    -> id:string
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> string
    -> string option t

  val update_versioned :
       ?should_exit:(unit -> unit t)
    -> ?upsert:bool
    -> index:string
    -> kind:string
    -> id:string
    -> version:int
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> Yojson.Basic.t
    -> bool t

  val get_id :
       index:string
    -> kind:string
    -> ?should_exit:(unit -> unit t)
    -> ?verbose:bool
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> string
    -> string option t

  val get_id_kinds :
       index:string
    -> ?kinds:string list
    -> ?should_exit:(unit -> unit t)
    -> ?verbose:bool
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> string
    -> string option t

  val multiget :
       ?index:string
    -> ?kind:string
    -> ?should_exit:(unit -> unit t)
    -> ?verbose:bool
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> Elastic_j.multiget_item list
    -> string option t

  val multiget_ids :
       index:string
    -> kind:string
    -> ?should_exit:(unit -> unit t)
    -> ?verbose:bool
    -> ?once:bool
    -> ?timeout:int
    -> ?retries:int
    -> ?args:(string * string) list
    -> string list
    -> string option t

  val retry_with_backoff' :
       ?init_delay:float
    -> ?multiplier:float
    -> ?variation:float
    -> 'a Enum.t
    -> 'a Enum.t
    -> (   ?f_fail:('b -> int -> unit)
        -> ?f_fail_all:('c -> 'd -> unit)
        -> 'a Enum.t
        -> unit t)
    -> unit t

  val retry_with_backoff :
       'a Enum.t
    -> 'a Enum.t
    -> (   ?f_fail:('b -> int -> unit)
        -> ?f_fail_all:('c -> 'd -> unit)
        -> 'a Enum.t
        -> unit t)
    -> unit t

  val request :
    ?pdebug:(string -> unit) ->
    ?version_conflict_handler:(string ->
                               string ->
                               int ->
                               string ->
                               string option t) ->
    name:string ->
    ?should_exit:(unit -> unit t) ->
    ?verbose:bool ->
    ?once:bool ->
    ?timeout:int ->
    ?retries:int ->
    ?body:string ->
    ?request_id:string ->
    Web.http_action ->
    string list ->
    ?args:(string * string) list ->
    unit ->
    string option t

  (** Bulk operations. *)

  type ('a, 'b) bulk_write_common =
       ?dry_run:bool
    -> ?name:string
    -> ?chunked:bool
    -> ?limit_bytes:int
    -> ?stats:string Cache.Count.t
    -> ?f_success:('a bulk_action -> unit)
    -> ?f_fail:(int option * 'a bulk_action -> string -> int -> bool)
    -> ?f_fail_all:(string -> int -> bool)
    -> ?f_retry:('a bulk_action -> unit)
    -> ?f_fail_retry:('a bulk_action -> unit)
    -> ?action_key_mode:action_key_mode
    -> ?connecttimeout:Time.t
    -> ?timeout:int
    -> ?args:(string * string) list
    -> ?retry:int
    -> ?check_conflict:bool
    -> ?loop:('a, unit t) bulk_write_loop
    -> 'b Enum.t
    -> unit t

  val bulk_write' : ('a, 'a bulk_action) bulk_write_common

  val bulk_write : (unit, command) bulk_write_common
end

module type Scroll_hit = sig
  type t

  val unformat : string -> t Elastic_t.scroll_hits_generic
end

module type Scroller = sig
  type t

  type process_batch_result = Continue | Stop | Restart

  type process_batch = int option * t list -> process_batch_result Lwt.t

  exception Wrapped of exn

  val scroll_batch_lwt :
       hosts:(module Hosts)
    -> ?request_timeout:int
    -> ?preference:string list
    -> ?args:(string * string) list
    -> ?name:string
    -> ?verbose:bool
    -> ?timeout:float
    -> ?size:int
    -> ?limit:int
    -> ?ranges:(string * int list) list
    -> index:string
    -> ?kind:string
    -> make_query:(unit -> Elastic_query_dsl.top_query)
    -> process_batch
    -> unit Lwt.t

  val scroll_stream_lwt' :
       hosts:(module Hosts)
    -> ?name:string
    -> ?verbose:bool
    -> ?request_timeout:int
    -> ?on_shard_failure:[< `Continue
                         | `Fail of exn
                         | `Restart of unit -> unit
                         | `Stop > `Continue ]
    -> ?args:(string * string) list
    -> ?preference:string list
    -> ?timeout:float
    -> ?size:int
    -> ?limit:int
    -> ?ranges:(string * int list) list
    -> index:string
    -> ?kind:string
    -> make_query:(unit -> Elastic_query_dsl.top_query)
    -> (t -> unit Lwt.t)
    -> unit Lwt.t

  val scroll_stream_lwt :
       hosts:(module Hosts)
    -> ?name:string
    -> ?verbose:bool
    -> ?request_timeout:int
    -> ?on_shard_failure:[< `Continue
                         | `Fail of exn
                         | `Restart of unit -> unit
                         | `Stop > `Continue ]
    -> ?args:(string * string) list
    -> ?preference:string list
    -> ?timeout:float
    -> ?size:int
    -> ?limit:int
    -> ?ranges:(string * int list) list
    -> index:string
    -> ?kind:string
    -> query:Elastic_query_dsl.top_query
    -> (t -> unit Lwt.t)
    -> unit Lwt.t

  val scroll_worker_lwt :
       hosts:(module Hosts)
    -> ?name:string
    -> ?verbose:bool
    -> ?request_timeout:int
    -> ?on_shard_failure:[< `Continue
                         | `Fail of exn
                         | `Restart of unit -> unit
                         | `Stop > `Continue ]
    -> ?args:(string * string) list
    -> ?queue_size:int
    -> ?batch_size:int
    -> ?timeout:float
    -> ?size:int
    -> ?limit:int
    -> ?ranges:(string * int list) list
    -> index:string
    -> ?kind:string
    -> query: Elastic_query_dsl.top_query
    -> (t list -> unit Lwt.t)
    -> unit Lwt.t
end
