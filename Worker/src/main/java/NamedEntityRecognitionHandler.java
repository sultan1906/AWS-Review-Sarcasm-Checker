import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class NamedEntityRecognitionHandler {
    private StanfordCoreNLP nerPipeline;

    public NamedEntityRecognitionHandler() {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        nerPipeline = new StanfordCoreNLP(props);
    }

    public String printEntities(String review) {
        StringBuilder answer = new StringBuilder("[");
        Annotation document = new Annotation(review);
        nerPipeline.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                if ("PERSON".equals(ne) || "LOCATION".equals(ne) || "ORGANIZATION".equals(ne)) {
                    answer.append(" ").append(word).append(":").append(ne).append(",");
                }
            }
        }
        if (answer.length() > 1) {
            answer.deleteCharAt(answer.length() - 1); // Remove the last comma
        }
        answer.append("]");
        return answer.toString();
    }
}
