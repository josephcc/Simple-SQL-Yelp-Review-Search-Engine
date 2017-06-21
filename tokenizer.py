from nltk.tokenize.treebank import TreebankWordTokenizer

class TreebankSpanTokenizer(TreebankWordTokenizer):

    def __init__(self):
        self._word_tokenizer = TreebankWordTokenizer()

    def span_tokenize(self, text):
        ix = 0
        for word_token in self.tokenize(text):
            ix = text.find(word_token, ix)
            end = ix+len(word_token)
            yield (ix, end)
            ix = end

    def tokenize(self, text, withSpans=False):
        tokens = self._word_tokenizer.tokenize(text)

        if not withSpans:
            return tokens

        spans = []
        ix = 0
        for word_token in tokens:
            ix = text.find(word_token, ix)
            end = ix+len(word_token)
            spans.append((ix, end))
            ix = end

        return zip(tokens, spans)

