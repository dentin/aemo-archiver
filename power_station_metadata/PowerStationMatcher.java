import java.io.*;
import java.util.*;

// Brute force power station to location matcher
public class PowerStationMatcher {

    static final String AEMO_GENERATORS_CSV = "nem-Generators and Scheduled Loads.csv";
    static final String GA_STATION_LOCATIONS_CSV = "GA_power_station_locations.csv";
    static final String MANUAL_LIST = "manual_station_locations.csv";


    public static void main(String[] args) throws Exception {
        final Map<String, String> stations = readCsv(AEMO_GENERATORS_CSV, 1, 13);
        final Map<String, String> locations = readCsv(GA_STATION_LOCATIONS_CSV, 0, 1);
        final Map<String, String> manualLocations = readCsv(MANUAL_LIST, 0, 1);

        // first remove matches we already have in the manual list from the AEMO station list
        for (Iterator<Map.Entry<String, String>> it = stations.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> e = it.next();
            if (manualLocations.containsKey(e.getValue())) {
                it.remove();
            }
        }

        // look for AEMO names with matches in the GA list
        for (Iterator<Map.Entry<String, String>> it = stations.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> e = it.next();
            final String loc = findMatchingKey(locations, e.getKey());
            if (loc != null) {
                System.out.println(e.getValue() + "," + loc);
                it.remove();
            }
        }

        // look for GA names matching in the AEMO list
        for (Map.Entry<String, String> e : locations.entrySet()) {
            final String station = findMatchingKey(stations, e.getKey());
            if (station != null) {
                System.out.println(station + "," + e.getValue());
            }
        }

        // find the rest using the distance
        for (Map.Entry<String, String> e : stations.entrySet()) {
            System.out.println(e.getValue() + "," + minDistanceLocation(locations, e.getKey(), e.getValue()));
        }
    }

    // read two columns of a CSV into a key/value map - not fully CSV compliant and filters certain things out
    public static Map<String, String> readCsv(final String filename, final int keyCol, final int valueCol) throws Exception {
        final BufferedReader r = new BufferedReader(new FileReader(filename));
        final Map<String, String> m = new HashMap<>();
        for (String line; (line = r.readLine()) != null; ) {
            final String[] parts = line.split(",");

            // sanitise
            final String value = parts[valueCol];
            if ("-".equals(value)) continue;
            if (value.trim().isEmpty()) continue;
            String key = parts[keyCol].replaceAll("[\"/()]", ""); // remove quotes

            m.put(key.trim(), parts[valueCol]);
        }
        r.close();
        return m;
    }

    public static String findMatchingKey(final Map<String, String> m, final String key) {
        // straight lookup - 95 matches
        if (m.containsKey(key)) return m.get(key);

        // cut key down to less and less words - inaccurate but adds about 10 matches
        // this would do better if we also cut down the map keys to be the same number of words
        String part = key;
        while (true) {
            final int index = part.lastIndexOf(" ");
            if (index > 0) {
                part = part.substring(0, index);
                if (m.containsKey(part)) return m.get(part);
            }
            else break;
        }

        return null;
    }

    public static String minDistanceLocation(final Map<String, String> m, final String name, final String duid) {
        final SortedMap<Integer, String> distances = new TreeMap<>();
        for (Map.Entry<String, String> e : m.entrySet()) {
            final int distance = LevenshteinDistance.computeLevenshteinDistance(name, e.getKey());
            distances.put(distance, e.getKey());
        }
        final int lowest = distances.firstKey();
        final String locationName = distances.get(lowest);
        final String location = m.get(locationName);
        System.out.println(lowest + ": " + duid + ", " + name + " = " + locationName);
        return location;
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
}
