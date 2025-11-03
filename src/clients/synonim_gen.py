from nltk.corpus import wordnet as wn
import nltk

# Solo la primera vez
nltk.download('wordnet')
nltk.download('omw-1.4')

def generate_synonyms(word: str, max_out: int = 15) -> list[str]:
    """Devuelve sinónimos básicos en inglés usando WordNet."""
    synonyms = set()
    for syn in wn.synsets(word):
        for lemma in syn.lemmas():
            synonyms.add(lemma.name().replace("_", " "))
    return list(synonyms)[:max_out]
