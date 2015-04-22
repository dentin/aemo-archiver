import java.io.*;
import java.util.*;

// Brute force power station to location matcher
public class PowerStationMatcher {

    /**
     * This takes 3 command line arguments:
     *   0: manual_station_locations.csv
     *   1: nem-Generators and Scheduled Loads.csv
     *   2: GA_power_station_locations.csv
     */
    public static void main(String[] args) throws Exception {
        final Map<String, String> manualLocations = readCsv(args[0], 0, 1);
        final Map<String, String> stations = readCsv(args[1], 1, 13, 1); // AEMO Generators list has a header
        final Map<String, String> locations = readCsv(args[2], 0, 1);

        // first remove matches we already have in the manual list from the AEMO station list
        for (Iterator<Map.Entry<String, String>> it = stations.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> e = it.next();
            if (manualLocations.containsKey(e.getValue())) {
                System.out.println(e.getValue() + "," + manualLocations.get(e.getValue()) + ",manual location");
                it.remove();
            }
        }

        // look for AEMO names with matches in the GA list
        for (Iterator<Map.Entry<String, String>> it = stations.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> e = it.next();
            final Match loc = findMatchingKey(locations, e.getKey());
            if (loc != null) {
                System.out.println(e.getValue() + "," + loc.value + ",'" + e.getKey() + "' = '" + loc.key + "'");
                it.remove();
            }
        }

        // look for GA names matching in the AEMO list
        for (Map.Entry<String, String> e : locations.entrySet()) {
            final Match station = findMatchingKey(stations, e.getKey());
            if (station != null) {
                System.out.println(station.value + "," + e.getValue() + ",'" + e.getKey() + "' = '" + station.key + "'");
            }
        }

        // find the rest using the Levenshtein distance
        for (Map.Entry<String, String> e : stations.entrySet()) {
            final Match m = minDistanceLocation(locations, e.getKey(), e.getValue());
            System.out.println(e.getValue() + "," + m.value + ",'" + e.getKey() + "' = '" + m.key + "'");
        }
    }

    public static Map<String, String> readCsv(final String filename, final int keyCol, final int valueCol) throws Exception {
        return readCsv(filename, keyCol, valueCol, 0);
    }

    // read two columns of a CSV into a key/value map - not fully CSV compliant and filters certain things out
    public static Map<String, String> readCsv(final String filename, final int keyCol, final int valueCol, final int discardNumInit)
            throws Exception {

        final BufferedReader r = new BufferedReader(new FileReader(filename));

        // discard any initial (header) lines
        if (discardNumInit > 0) {
            for (int i = 0; i < discardNumInit; i++) {
                r.readLine();
            }
        }

        final Map<String, String> m = new HashMap<>();
        for (String line; (line = r.readLine()) != null; ) {
            // there are columns with commas in them, we need to remove them so we don't screw up splitting
            // TODO: refactor CSV reading to use a robust library!
            line = line.replaceAll(", ", " ");

            final String[] parts = line.split(",");

            // sanitise
            String value = parts[valueCol];
            if ("-".equals(value)) continue;
            if (value.trim().isEmpty()) continue;
            value = value.replaceAll("\"", "");
            String key = parts[keyCol].replaceAll("[\"()]", ""); // remove quotes
            key = key.replaceAll(" (Gas Power Station|Power Station|Station|Power Plant|Generation Plant|Plant)", "");
            key = key.replaceAll(" (Wind Farm|Solar Farm|Solar Park)", "");
            key = key.replaceAll(" (Waste Disposal Facility|Renewable Energy Facility|Energy Facility|Facility)", "");
            key = key.replaceAll(" (Gas Turbine|Landfill|Hydro|GT)", "");

            m.put(key.trim(), value.trim());
        }
        r.close();
        return m;
    }

    public static Match findMatchingKey(final Map<String, String> m, final String key) {
        // straight lookup - 95 matches
        if (m.containsKey(key)) return new Match(key, m.get(key));

        // cut key down to less and less words - inaccurate but adds about 10 matches
        // this would do better if we also cut down the map keys to be the same number of words
        String part = key;
        while (true) {
            final int index = part.lastIndexOf(" ");
            if (index > 0) {
                part = part.substring(0, index);
                if (m.containsKey(part)) return new Match(part, m.get(part));
            }
            else break;
        }

        return null;
    }

    public static Match minDistanceLocation(final Map<String, String> m, final String name, final String duid) {
        final SortedMap<Integer, String> distances = new TreeMap<>();
        for (Map.Entry<String, String> e : m.entrySet()) {
            final int distance = LevenshteinDistance.computeLevenshteinDistance(name, e.getKey());
            distances.put(distance, e.getKey());
        }
        final int lowest = distances.firstKey();
        final String locationName = distances.get(lowest);
        final String location = m.get(locationName);
        return new Match(locationName, location);
    }


    private static class LevenshteinDistance {
        private static int minimum(final int a, final int b, final int c) {
            return Math.min(Math.min(a, b), c);
        }

        public static int computeLevenshteinDistance(final String str1, final String str2) {
            int[][] distance = new int[str1.length() + 1][str2.length() + 1];

            for (int i = 0; i <= str1.length(); i++)
                distance[i][0] = i;
            for (int j = 1; j <= str2.length(); j++)
                distance[0][j] = j;

            for (int i = 1; i <= str1.length(); i++)
                for (int j = 1; j <= str2.length(); j++)
                    distance[i][j] = minimum(
                            distance[i - 1][j] + 1,
                            distance[i][j - 1] + 1,
                            distance[i - 1][j - 1] + ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0 : 1));

            return distance[str1.length()][str2.length()];
        }
    }

    private static class Match {
        final String key;
        final String value;

        Match(final String k, final String v) {
            key = k;
            value = v;
        }
    }
}
