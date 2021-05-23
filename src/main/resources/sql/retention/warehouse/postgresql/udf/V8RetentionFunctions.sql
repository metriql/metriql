CREATE EXTENSION IF NOT EXISTS plv8;
create or replace function analyze_retention_intermediate(arr integer[], ff boolean[]) returns integer[] volatile language plv8 as $$

 if(ff == null) return arr;
 for(var i=0; i <= ff.length; i++) {
   if(ff[i] === true) {
   arr[i] = arr[i] == null ? 1 : (arr[i]+1)
   }
 }

 return arr;
$$;

create or replace function generate_timeline(start date, arr date[], durationmillis bigint, max_step integer) returns boolean[] volatile language plv8 as $$

 if(arr == null) {
  return null;
 }
 var steps = null;

 for(var i=0; i <= arr.length; i++) {
   var value = (arr[i]-start);
   if(value < 0) continue;

   var gap = value / durationmillis;
   if(gap > max_step) {
	break;
   }

   if(steps == null) {
       steps = new Array(Math.min(max_step, arr.length))
   }

  //plv8.elog(ERROR, gap, value, start, durationmillis);
   steps[gap] = true;
 }

 return steps;
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