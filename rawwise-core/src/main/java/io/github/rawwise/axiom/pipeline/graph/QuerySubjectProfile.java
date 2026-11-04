package io.github.rawwise.axiom.pipeline.graph;

import io.github.rawwise.axiom.pipeline.entity.Entity;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record QuerySubjectProfile(
        Set<String> subjectEntityIds, Set<String> topicEntityIds, boolean hasSubject) {
    public boolean isSubjectInDia(Set<String> diaEntities) {
        if (!hasSubject) return true;
        if (diaEntities == null || diaEntities.isEmpty()) return false;
        return diaEntities.stream().anyMatch(subjectEntityIds::contains);
    }

    public static QuerySubjectProfile resolve(
            String query, List<String> entityCandidates, GraphQuery graphQuery, String sampleId) {
        String subjectName = extractSubjectName(query);
        boolean hasSubjectName = subjectName != null;

        Set<String> subjectEntityIds = new HashSet<>();
        Set<String> topicEntityIds = new HashSet<>();

        if (entityCandidates != null && graphQuery != null) {
            for (String name : entityCandidates) {
                try {
                    Optional<Entity> entity = graphQuery.findEntity(sampleId, name);
                    if (entity.isEmpty()) continue;
                    if (hasSubjectName && name.equalsIgnoreCase(subjectName)) {
                        subjectEntityIds.add(entity.get().id());
                    } else {
                        topicEntityIds.add(entity.get().id());
                    }
                } catch (RuntimeException e) {
                    // skip on error - fallback to no-subject
                }
            }
        }

        boolean hasSubject = hasSubjectName && !subjectEntityIds.isEmpty();
        return new QuerySubjectProfile(subjectEntityIds, topicEntityIds, hasSubject);
    }

    private static final Pattern[] SUBJECT_PATTERNS = {
        Pattern.compile(
                "^(?:what|who|how|when|where|why)\\s+(?:did|does|do|was|were|is|are)\\s+(\\w+)\\b",
                Pattern.CASE_INSENSITIVE),
        Pattern.compile("^(\\w+)['\u2019]s\\b", Pattern.CASE_INSENSITIVE),
        Pattern.compile("^how\\s+many\\s+times\\s+did\\s+(\\w+)\\b", Pattern.CASE_INSENSITIVE),
    };

    private static String extractSubjectName(String query) {
        if (query == null || query.isBlank()) return null;
        for (Pattern p : SUBJECT_PATTERNS) {
            Matcher m = p.matcher(query);
            if (m.find()) {
                String name = m.group(1);
                if (!name.isEmpty() && Character.isUpperCase(name.charAt(0))) {
                    return name;
                }
            }
        }
        return null;
    }
}
