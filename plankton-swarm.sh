#!/bin/bash

show_help() {
  cat <<EOF
Plankton are fascinating sea creatures that drift with ocean currents, playing a vital role in moving organisms through waters and maintaining the delicate balance of marine ecosystems.
Inspired by their natural harmony, "Plankton" becomes the perfect name for balancing data movement across distributed data nodes.

This is a simple basic manual balancer to move replicated pgs from most full OSDs to least full ones with node failure domain.
Upon run it will generate a 'swarm-file' that you can review and execute.
This balancer works best in clusters with a large number of nodes due to default node failure domain.

Usage: plankton-swarm.sh source-osds [...] target-osds [...] pgs [...] 
       (optional flags: keep-upmaps/check-upmaps, respect-avg-pg)

Examples:
1. Move PGs from specific OSDs to specific OSDs:

   ./plankton-swarm.sh source-osds osd.1,osd.2 target-osds osd.3,osd.4 pgs 5
   # Moves 5 PGs from OSDs 1,2 to OSDs 3,4

2. Move PGs from overutilized OSDs to specific OSDs:

   ./plankton-swarm.sh source-osds gt85 target-osds 1,2 pgs 10
   # Moves 10 PGs from OSDs above 85% utilization to OSDs osd.1,osd.2
   # Uses default top 3 most utilized OSDs as source

3. Move PGs from overutilized OSDs to underutilized OSDs:

   ./plankton-swarm.sh source-osds 85-90 target-osds lt60 pgs 7
   # Moves 7 PGs from OSDs between 85% and 90% to OSDs below 60% utilization
   # Uses default top 3 most utilized OSDs as source

4. Move PGs from specific OSDs to OSDs within utilization range:

   ./plankton-swarm.sh source-osds osd.1,osd.2 target-osds 40-60 pgs 10
   # Moves 10 PGs from OSDs 1,2 to OSDs with utilization between 40% and 60%

5. Specify number of source/target OSDs with thresholds:

   ./plankton-swarm.sh source-osds gt88 10 target-osds lt65 5 pgs 5 keep-upmaps
   # Moves 5 PGs from top 10 OSDs above 88% to 5 least utilized OSDs below 65%
   # Keeps existing upmaps by skipping pgs that already have upmap

6. Use top 5 OSDs as source:

   ./plankton-swarm.sh source-osds top 5
   # Move 3 PGs from top 5 used OSDs to least uitilized OSDs below 65% 

6. Use all overutilized OSDs as source:

   ./plankton-swarm.sh source-osds gt85 all target-osds lt60 pgs 10
   # Moves 10 PGs from ALL OSDs above 85% to OSDs below 60%

Threshold Operators:
- gt: greater than (e.g., gt85 = >85%)
- lt: less than (e.g., lt60 = <60%)
- ge: greater than or equal to
- le: less than or equal to
- eq: equal to
- Range format: min-max (e.g., 40-60 = between 40% and 60%)

By default plankton-swarm does not care about existing pg upmaps. To change this use upmap flags:
- keep-upmaps   will skip pgs with any existing upmaps. Safest option.
- check-upmaps   will check for existing pg upmaps and add their OSDs to node failure domain check.

Notes:
- OSD IDs can be specified with or without 'osd.' prefix. Usage configurations can be passed in any order.
- Keywords 'top' and 'all' are only valid for source-osds, not target-osds.
- Default number of source OSDs is 3 if not specified. Default value is 3 pgs to move if not specified.
- Default threshold for target osds is <65%. If target-osds not specified it will move pgs to all OSDs below 65%.
- When using thresholds, displayed OSDs are sorted by utilization (highest first for source, lowest first for target).
- Use 'respect-avg-pg' flag to find target OSDs with below average pg count (experimental).
- To generate a larger list of possible pgs a pg_pre_fetch multiplier is used. Increase it in case you run into a lot of 'Skipping.' messages for pgs.
- Experimental: flag 'sort-by-pgs' will sort source OSDs by the number of pgs in each OSD. This feature is still WIP.

EOF
}

# Default values - change if needed
pg_limit=3
top_n_osds=3
underused_threshold=65
balance_file="swarm-file"

pg_pre_fetch=3  # generates a larger list of possible pgs to move (pg_pre_fetch * pg_limit). Increase if you get a lot of 'Skipping.' for pgs.
check_upmaps=false  # 'true' (check-upmaps flag) will check for existing pg upmaps and remove them from generated target OSD list
keep_upmaps=false  # 'true' (keep-upmaps flag) will skip pgs if they already have upmaps
skip_avg_pgs=true  # 'false' (respect-avg-pg flag) generates target OSD list including OSDs with below average PG count


custom_osds=""
target_osds=""
overused_threshold=""
source_top_n=""
target_top_n=""
target_range_min=""
target_range_max=""

strip_osd_prefix() {
    echo "$1" | sed 's/osd\.//g'
}

is_threshold() {
    local value="$1"
    [[ "$value" =~ ^(gt|lt|ge|le|eq)[0-9]+$ ]] || [[ "$value" =~ ^[0-9]+-[0-9]+$ ]]
    return $?
}

parse_threshold() {
    local value="$1"
    
    if [[ "$value" =~ ^([0-9]+)-([0-9]+)$ ]]; then
        echo "range:${BASH_REMATCH[1]}-${BASH_REMATCH[2]}"
        return
    fi

    local number="${value#??}"
    local operator="${value:0:2}"
    
    case "$operator" in
        gt) echo ">$number" ;;
        lt) echo "<$number" ;;
        ge) echo ">=$number" ;;
        le) echo "<=$number" ;;
        eq) echo "=$number" ;;
        *) echo "Error: Unknown operator in: $value" >&2; return 1 ;;
    esac
}

# Parse input options and values
while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h|help)
            show_help
            exit 0
            ;;
        source-osds)
            shift
            value="$1"
            if is_threshold "$value"; then
                overused_threshold="$(parse_threshold "$value")"
                if [[ "$overused_threshold" =~ range:([0-9]+)-([0-9]+) ]]; then
                    source_range_min="${BASH_REMATCH[1]}"
                    source_range_max="${BASH_REMATCH[2]}"
                fi
            elif [[ $value == "top" ]]; then
                overused_threshold=0
            else
                custom_osds="$(strip_osd_prefix "$value")"
            fi
            shift
            if [[ -n "$overused_threshold" && $# -gt 0 && ("$1" =~ ^[0-9]+$ || "$1" == "all") ]]; then
                top_n_osds="$1"
                shift
            fi
            ;;
        target-osds)
            shift
            value="$1"
            if is_threshold "$value"; then
                underused_threshold="$(parse_threshold "$value")"
                if [[ "$underused_threshold" =~ range:([0-9]+)-([0-9]+) ]]; then
                    target_range_min="${BASH_REMATCH[1]}"
                    target_range_max="${BASH_REMATCH[2]}"
                fi
            else
                target_osds="$(strip_osd_prefix "$value")"
                underused_threshold=""
            fi
            shift
            if [[ -n "$underused_threshold" && $# -gt 0 && "$1" =~ ^[0-9]+$ ]]; then
                target_top_n="$1"
                shift
            fi
            ;;
        pgs)
            shift
            if [[ $# -eq 0 || ! "$1" =~ ^[0-9]+$ ]]; then
                echo "Error: pgs requires a numeric argument"
                exit 1
            fi
            pg_limit="$1"
            shift
            ;;
        respect-avg-pg)
            skip_avg_pgs=false
            shift
            ;;
        check-upmaps)
            check_upmaps=true
            shift
            ;;
        keep-upmaps)
            check_upmaps=true
            keep_upmaps=true
            shift
            ;;
        sort-by-pgs)
            by_pgs=true
            shift
            ;;
        *)
            show_help
            exit 1
            ;;
    esac
done

echo "$(date '+%Y-%m-%d %H:%M:%S'): Gathering plankton."

# Get source osds
if [[ -n "$custom_osds" ]]; then
    overused_osds="$custom_osds"
    echo "Using custom source OSDs: $overused_osds"

elif [[ -n "$source_range_min" && -n "$source_range_max" ]]; then
    overused_osds=$(ceph osd df -f json | jq -r --argjson min "$source_range_min" --argjson max "$source_range_max" --arg top_n "$top_n_osds" '
        .nodes
        | map(select(.utilization >= $min and .utilization <= $max))
        | sort_by(-.utilization)
        | (if $top_n == "all" then . else .[:($top_n | tonumber)] end)
        | map(.id)
        | join(",")
    ')
    echo "Source OSDs (${source_range_min}%-${source_range_max}%): $overused_osds"
elif [[ -n "$overused_threshold" ]]; then
    threshold_value=$(echo "$overused_threshold" | tr -dc '0-9')
    # Experimental: added sorting by pgs with sort-by-pgs
    overused_osds=$(ceph osd df -f json | jq -r --argjson threshold "$threshold_value" --arg top_n "$top_n_osds" --arg by_pgs "${by_pgs:-}" '
        .nodes
        | map(select(if $by_pgs == "true" then .pgs >= $threshold else .utilization >= $threshold end))
        | sort_by(if $by_pgs == "true" then -(.pgs) else -(.utilization) end)
        | (if $top_n == "all" then . else .[:($top_n | tonumber)] end)
        | map(.id)
        | join(",")
    ')
    
    if [[ "$top_n_osds" == "all" ]]; then
        echo "All overused OSDs (>$threshold_value%): $overused_osds"
    else
        echo "$top_n_osds overused OSDs (>$threshold_value%): $overused_osds"
    fi
    
    if [[ -z "$overused_osds" ]]; then
        echo "No overused OSDs above $threshold_value% found."
        exit 0
    fi
else
    echo "Error: no source OSDs."
    exit 1
fi
source_osd_count=$(echo "$overused_osds" | tr ',' '\n' | wc -l)

# Get target osds
if [[ -n "$target_osds" ]]; then
    underused_osds="$target_osds"
    echo "Using custom target OSDs: $underused_osds"
elif [[ -n "$target_range_min" && -n "$target_range_max" ]]; then
    # In below code using ($source_osd_count / 2) just to have some more target OSDs than source in case average pg count is respected.
    # This might need more improvement in the future, especially in case movement happens to a single OSD.
    underused_osds=$(ceph osd df -f json | jq -r --argjson min "$target_range_min" --argjson max "$target_range_max" --argjson skip_avg_pgs "$skip_avg_pgs" --argjson source_count "$source_osd_count" '
        .nodes
        | map(.pgs) as $pgs_list
        | ($pgs_list | add / length) as $avg_pgs
        | map(select(.utilization >= $min and .utilization <= $max and ($skip_avg_pgs or .pgs < $avg_pgs))) as $filtered_nodes
        | (if $filtered_nodes | length < ($source_count / 2) then
            map(select(.utilization >= $min and .utilization <= $max)) 
          else 
            $filtered_nodes
          end)
        | sort_by(.utilization)
        | map(.id)
        | join(",")
    ')
    echo "Target OSDs (${target_range_min}%-${target_range_max}%): $underused_osds"
elif [[ -n "$underused_threshold" ]]; then
    threshold_value=$(echo "$underused_threshold" | tr -dc '0-9')

    underused_osds=$(ceph osd df -f json | jq -r --argjson threshold "$threshold_value" --argjson skip_avg_pgs "$skip_avg_pgs" --argjson source_count "$source_osd_count" '
        .nodes
        | map(.pgs) as $pgs_list
        | ($pgs_list | add / length) as $avg_pgs
        | map(select(.utilization < $threshold and ($skip_avg_pgs or .pgs < $avg_pgs))) as $filtered_nodes
        | (if $filtered_nodes | length < ($source_count / 2) then
            map(select(.utilization < $threshold)) 
          else 
            $filtered_nodes
          end)
        | sort_by(.utilization)
        | map(.id)
        | join(",")
    ')
    echo "Underused OSDs ($underused_threshold%): $underused_osds"
else
    echo "Error: no target OSDs."
    exit 1
fi

# Find ways to move pgs
echo "Will now find ways to move $pg_limit pgs in each OSD respecting node failure domain."
> "$balance_file"

IFS=',' read -ra overused_list <<< "$overused_osds"
IFS=',' read -ra underused_list <<< "$underused_osds"

json_data=$(ceph osd tree -f json)
[[ $check_upmaps == true ]] && mapfile -t map_items < <(ceph osd dump | grep pg_upmap_items)

for osd in "${overused_list[@]}"; do
    echo "Processing OSD $osd..."

    pgs=$(ceph pg dump | grep -E ",$osd]|,$osd," | grep -P 'active\+clean(?!\+)' | awk '{print $1, $19}' | head -n "$((pg_limit * pg_pre_fetch))" | shuf)

    if [[ -z "$pgs" ]]; then
        echo "No active and clean pgs found for $osd. Skipping."
        continue
    fi

    pg_count=0
    while read -r pg acting_set && [[ "$pg_count" -lt "$pg_limit" ]]; do
        IFS=',' read -r -a acting_osds <<< "$(echo "$acting_set" | tr -d '[]')"
        shuffled_underused_osds=($(shuf -e "${underused_list[@]}"))

        declare -A osd_to_host_id
        for acting_osd in "${acting_osds[@]}"; do
            osd_to_host_id[$acting_osd]=$(echo "$json_data" | jq -r --argjson child_id "$acting_osd" '
                .nodes[] | select(.children[]? == $child_id) | .id
            ')
        done

            if $check_upmaps; then
                pg_upmaps=$(printf '%s\n' "${map_items[@]}" | grep -w "$(echo "$pg" | sed 's/\./\\./g')") 
                if [[ -n "$pg_upmaps" ]]; then
                    if $keep_upmaps; then
                        echo "PG $pg already has upmaps: $pg_upmaps. Skipping."
                        continue
                    else
                        upmap_osd_list=($(echo "$pg_upmaps" | grep -o '\[[^]]*\]' | tr -d '[]' | tr ',' ' '))
                        echo "PG $pg already has upmaps: ${upmap_osd_list[@]}. Adding them for node failure domain check."
                        for upmap_osd in "${upmap_osd_list[@]}"; do
                            osd_to_host_id[$upmap_osd]=$(echo "$json_data" | jq -r --argjson child_id "$upmap_osd" '
                                .nodes[] | select(.children[]? == $child_id) | .id
                            ')
                        done

                        declare -A unique_osds
                        for item in "${acting_osds[@]}" "${upmap_osd_list[@]}"; do
                            unique_osds["$item"]=1
                        done
                        acting_osds=("${!unique_osds[@]}")
                    fi
                fi
            fi
    
            selected_osd=""
            for new_osd in "${shuffled_underused_osds[@]}"; do
                new_host_id=$(echo "$json_data" | jq -r --argjson child_id "$new_osd" '
                    .nodes[] | select(.children[]? == $child_id) | .id
                ')

                host_conflict=false
                for acting_osd in "${acting_osds[@]}"; do
                    if [[ "${osd_to_host_id[$acting_osd]}" == "$new_host_id" ]]; then
                        host_conflict=true
                        echo "New mapping for $pg (osd.$acting_osd to osd.$new_osd) is breaking node failure domain. Retrying."
                    fi
                done

                [[ "$host_conflict" == true ]] && continue
                selected_osd="$new_osd"
            done

            if [[ -n "$selected_osd" ]]; then
                echo "ceph osd pg-upmap-items $pg $osd $selected_osd"
                echo "ceph osd pg-upmap-items $pg $osd $selected_osd" >> "$balance_file"
                ((pg_count++))
            else
                echo "No suitable OSD found for PG $pg. Sorry."
            fi
    done <<< "$pgs"
done

echo "$(date '+%Y-%m-%d %H:%M:%S'): Balance pgs commands written to $balance_file - review and let plankton swarm with 'bash $balance_file'."
