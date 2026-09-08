Builds German TSO-level solar target zarr from ENTSO-E.

Four files, to be run consecutively:

1. `de_fetch_entsoe_tso.py` - Downloads generation (psr_type B16) and
   installed capacity, for four German control areas. One parquet
   per TSO per year for generation, one per TSO for capacity.

2. `de_build_zarr.py` - Assembles parquets into a zarr at 15 min resolution.
   Capacity is reported installed capacity interpolated onto the same grid.
   Three checks and does not write if per-TSO generation shares are constant.

3. `de_build_capacity_envelope.py` - Replaces capacity with rolling 365 day p99.9
   of each TSO's own generation. The reported installed capacity counts behind the
   meter.

4. `de_build_national.py` - Adds `location_id` 0 as national total with the TSOs
   at 1-4. National capacity is the four TSO capacities summed as of now.
