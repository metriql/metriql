create or replace function generate_timeline(start date, arr date[], durationmillis bigint, max_step integer) returns boolean[] volatile language plpgsql as $$
DECLARE
 steps boolean[];
 value int;
 gap int;
 item date;
BEGIN
 if arr is null then
  return null;
 end if;

-- substracting dates returns an integer that represents date diff
durationmillis := durationmillis / 86400000;
FOREACH item IN ARRAY arr
LOOP
   value := (item - start);

   if value < 0 then
      continue;
   end if;

   gap := value / durationmillis;
   if gap > max_step then
      EXIT;
   end if;

   if steps is null then
       steps = cast(ARRAY[] as boolean[]);
       --steps = new Array(Math.min(max_step, arr.length))
   end if;

   steps[gap+1] := true;

 END LOOP;

 return steps;
END
$$;

create or replace function analyze_retention_intermediate(arr integer[], ff boolean[]) returns integer[] volatile language plpgsql as $$
DECLARE
 i int;
begin
 if ff is null then
    return arr;
 end if;

FOR i IN 1 .. array_upper(ff, 1)
 LOOP
   if ff[i] = true then
     if arr[i] is null then
       arr[i] := 1;
     ELSE
       arr[i] := (arr[i]+1);
     end if;
   end if;
END LOOP;
return arr;
END
$$;

DO $$ BEGIN
CREATE AGGREGATE collect_retention(boolean[])
(
    sfunc = analyze_retention_intermediate,
    stype = integer[],
    initcond = '{}'
);
EXCEPTION
    WHEN duplicate_function THEN NULL;
END $$;