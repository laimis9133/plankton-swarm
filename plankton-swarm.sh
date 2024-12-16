#!/bin/bash

show_help() {
  cat <<EOF
Plankton are fascinating sea creatures that drift with ocean currents, 
playing a vital role in moving organisms through waters and maintaining the delicate balance of marine ecosystems. 
Inspired by their natural harmony, "Plankton" becomes the perfect name for balancing data movement across distributed data nodes.

This is a simple basic manual balancer to move replicated pgs from most full OSDs to least full ones with node failure domain.

Usage:
  plankton-swarm.sh [overused_threshold] [pg_limit] [top_n_osds/all] [underused_threshold]
    - overused_threshold: utilization (%) above which OSDs are considered overused.
    - pg_limit: specify the number of PGs to move per OSD (default: 10).
    - top_n_osds: specify the top N overused OSDs to process (default: 3). Use 'all' flag instead for all overused OSDs.
    - underused_threshold: usage below which OSDs become targets (default: 65%).

  plankton-swarm.sh source-osds <osd1,osd2,...> [pg_limit]
    - Use a custom list of source OSDs.
    - Specify the number of PGs to move per OSD (default: 10).

Options:
  --help,help,-h               Show this help message and exit.

Examples:
  ./plankton-swarm.sh source-osds osd.1,osd.2
    - Use OSDs osd.1 and osd.2 as source - move 10 pgs to OSDs below 65% utilization (defaults).

  ./plankton-swarm.sh source-osds osd.3,osd.4 7
    - Use OSDs osd.3 and osd.4 as source - move 7 pgs to OSDs below 65%.

  ./plankton-swarm.sh 90 15 5 60
    - Detect overused OSDs above 90%, move 15 PGs from each of the top 5 OSDs to OSDs with below 60% utilization.

  ./plankton-swarm.sh 91 5 all
    - Detect all overused OSDs above 91% usage, move 5 PGs from each one to OSDs below 65% (default) utilization.

EOF
}

# Default values
pg_limit=10
top_n_osds=3
underused_threshold=65

custom_osds=""
overused_threshold=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --help|-h|help)
            show_help
            exit 0
            ;;
        source-osds)
            custom_osds="$2"
            shift 2
            if [[ $# -gt 0 ]]; then
                pg_limit="$1"
                shift
            fi
            ;;
        *)
            if [[ -z "$overused_threshold" ]]; then
                overused_threshold="$1"
                
                if [[ $# -gt 1 ]]; then
                    pg_limit="${2:-$pg_limit}"
                    shift
                fi
                
                if [[ $# -gt 1 ]]; then
                    top_n_osds="${2:-$top_n_osds}"
                    shift
                fi
                
                if [[ $# -gt 1 ]]; then
                    underused_threshold="${2:-$underused_threshold}"
                    shift
                fi
            else
                echo "Unexpected argument: $1"
                show_help
                exit 1
            fi
            ;;
    esac
    shift
done

if [[ -z "$custom_osds" && -z "$overused_threshold" ]]; then
  echo "Error: Missing required arguments."
  show_help
  exit 1
fi


if [[ -n "$custom_osds" ]]; then
  overused_osds="$custom_osds"
  echo "Using custom source OSDs: $overused_osds"
else
  overused_osds=$(ceph osd df -f json | jq -r --argjson threshold "$overused_threshold" --arg top_n "$top_n_osds" '
    .nodes 
    | map(select(.utilization >= $threshold)) 
    | sort_by(-.utilization) 
    | (if $top_n == "all" then . else .[:$top_n | tonumber] end) 
    | map(.id) 
    | join(",")
  ')
  echo "$top_n_osds overused OSDs (>$overused_threshold%): $overused_osds"

  if [[ -z "$overused_osds" ]]; then
    echo "No overused OSDs above $overused_threshold% found."
    exit 0
  fi
fi

underused_osds=$(ceph osd df -f json | jq -r --argjson threshold "$underused_threshold" '
  .nodes 
  | map(select(.utilization < $threshold)) 
  | sort_by(.utilization) 
  | map(.id) 
  | join(",")
')
echo "Underused OSDs (<$underused_threshold%): $underused_osds"

echo "Will now find ways to move $pg_limit pgs in each OSD respecting node failure domain."

balance_file="swarm-file"
> "$balance_file"


IFS=',' read -ra overused_list <<< "$overused_osds"
IFS=',' read -ra underused_list <<< "$underused_osds"

json_data=$(ceph osd tree -f json)

for osd in "${overused_list[@]}"; do
  echo "Processing OSD $osd..."

  pgs=$(ceph pg dump | grep ",$osd]" | grep -P 'active\+clean(?!\+)' | awk '{print $1, $19}' | head -n "$pg_limit")

  while read -r pg acting_set; do
    IFS=',' read -r -a acting_osds <<< "$(echo "$acting_set" | tr -d '[]')"

    declare -A osd_to_host_id
    for acting_osd in "${acting_osds[@]}"; do
      osd_to_host_id[$acting_osd]=$(echo "$json_data" | jq -r --argjson child_id "$acting_osd" '
        .nodes[] | select(.children[]? == $child_id) | .id
      ')
    done

    shuffled_underused_osds=($(shuf -e "${underused_list[@]}"))

    selected_osd=""
    for new_osd in "${shuffled_underused_osds[@]}"; do
      new_host_id=$(echo "$json_data" | jq -r --argjson child_id "$new_osd" '
        .nodes[] | select(.children[]? == $child_id) | .id
      ')

      host_conflict=false
      for acting_osd in "${acting_osds[@]}"; do
        if [[ "${osd_to_host_id[$acting_osd]}" == "$new_host_id" ]]; then
          host_conflict=true
          echo "New mapping for osd.$osd to osd.$acting_osd is breaking failure domain (host) - retrying."
          break
        fi
      done

      if [[ "$host_conflict" == false ]]; then
        selected_osd="$new_osd"
        break
      fi
    done

    if [[ -n "$selected_osd" ]]; then
      echo "ceph osd pg-upmap-items $pg $osd $selected_osd"
      echo "ceph osd pg-upmap-items $pg $osd $selected_osd" >> "$balance_file"
    else
      echo "No suitable OSD found for PG $pg from OSD $osd"
    fi
  done <<< "$pgs"
done

echo "Balance pgs commands written to $balance_file - review and run it with 'bash $balance_file'."
