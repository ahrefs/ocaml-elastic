open Devkit

let log = Log.from "elastic"

(**
  Build a function to serialize an enum in chunks, returning no more than specified number of bytes each time it is called.
  If a limit_bytes is provided, the only part of enum is consumed that seralizes into not more than the limit_bytes. It is then
  the caller's responsibility to check whether the enum was fully consumed or not after the function returns an empty chunk.
  @param enum to serialize in chunks
  @param make serialization function for the enum
  @param mark function to call for consumed elements
  @param limit_bytes output to the specified number of bytes
  @param strict_limit controls whether the last element that does not fit the limit is included or not
  @return function that takes the maximum size of chunk to return
*)
let chunked_read enum ?limit_bytes ?(strict_limit = false) ?mark make =
  let buf = Buffer.create 1024 in
  let limit = Option.map ref limit_bytes in
  let enum = Enum.map (fun elem -> elem, make elem) enum in
  (* fill up the buffer *)
  let rec fill n =
    match Buffer.length buf, limit with
    | len, _ when len >= n -> ()
    | len, Some limit when len >= !limit -> ()
    | len, _ ->
    match Enum.peek enum with
    | None -> ()
    | Some (elem, str) ->
      let len = len + String.length str in
      ( match limit with
      | Some limit when (strict_limit && len > !limit) || !limit <= 0 -> ()
      | _ ->
        Enum.junk enum;
        call_me_maybe mark elem;
        Buffer.add_string buf str;
        fill n
      )
  in
  fun n ->
    try
      fill n;
      let len = Buffer.length buf in
      let n = min n len in
      ( match limit with
      | Some limit -> limit -= n
      | _ -> ()
      );
      (* flush *)
      let res = Buffer.sub buf 0 n in
      let tail = Buffer.sub buf n (len - n) in
      Buffer.clear buf;
      (* store tail *)
      Buffer.add_string buf tail;
      res
    with exn ->
      log#error ~exn "chunked_read";
      raise exn
