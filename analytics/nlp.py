# --------------------------------------------------------------------------------------------------------
# 2020/07/27
# src - nlp.py
# md
# --------------------------------------------------------------------------------------------------------
import io
import os
import random
import re
from datetime import datetime

import spacy
import textacy
from gensim.corpora import Dictionary
from gensim.models import Phrases
from gensim.models.phrases import Phraser
from spacy.lang.nl import STOP_WORDS
from textacy.extract import ngrams
from textacy.io import write_text, read_text
from textacy.preprocessing import normalize_quotation_marks, normalize_unicode, normalize_whitespace, replace_numbers, replace_urls, replace_emojis, replace_hashtags, \
    remove_accents, remove_punctuation
from transformers import RobertaTokenizer, BertTokenizer

from database.analytics_facade import get_tweets
from database.twitter_facade import get_usernames


class TweetCorpus:
    """
    - Tweets are not splitted in sentenses.
    - Tweets can be shuffled per user and users can be shuffled, not tweets from all users.
    - Emoji's with skin tones are split into emoji and modifier. This is not necessarely bad.
      Moreover, bigram connects emoji and modifier
    """

    def __init__(self):
        self.users_corpus_path = '/media/Development/Twitter_Analytics/src/analytics/data/users_corpora/'
        self.corpora_path = '/media/Development/Twitter_Analytics/src/analytics/data/corpora/'
        self.corpus_name = 'full_corpus'
        self.nlp = spacy.load('nl_core_news_lg',
                              disable=[
                                  'parser',
                                  'tagger',
                                  'ner'
                              ])
        self.nlp.max_length = self.nlp.max_length * 15

        # emoji = Emoji(self.nlp)
        # self.nlp.add_pipe(emoji, first=True)

    def make_users_copus(self, sample=None):
        t0 = datetime.now()
        usernames = get_usernames()
        if sample: usernames = usernames.sample(sample)

        print(usernames)
        for username in usernames['username']:
            # for username in ['a_blancquaert']:
            t1 = datetime.now()
            tweets_df = get_tweets(username)
            if tweets_df.empty: continue
            tweets = []
            for tweet in tweets_df['tweet']:
                tweet = self.normalize_tweet(tweet, hashtags=False, emojis=False)
                tweet = self.lemmatize(tweet, keep_hashtag=True)
                tweets.append(tweet)
            write_text(tweets, self.users_corpus_path + f'{username}.txt', lines=True, make_dirs=True)
            print(datetime.now() - t1, '-', username)
        print('Total time:', datetime.now() - t0)

    def make_big_corpus(self, max_char=None, shuffle=False, delete=True):
        max_char = max_char if max_char else 1e12
        filenames = os.listdir(self.users_corpus_path)
        if shuffle: random.shuffle(filenames)
        if delete and self.corpus_name + '.txt' in os.listdir(self.corpora_path):
            input(f'The corpus - {self.corpus_name + ".txt"} - will be deleted! Press enter to contine ...')
            os.remove(self.corpora_path + self.corpus_name + '.txt')
        while 1:
            for filename in filenames:
                print(filename)
                tweets = list(read_text(self.users_corpus_path + filename, mode='r', lines=True))
                for tweet in tweets:
                    write_text(tweet, self.corpora_path + self.corpus_name + '.txt', mode='at', lines=False, make_dirs=True)
                    max_char -= len(tweet)
                    if max_char < 0: break
                if max_char < 0: break
            break

    @staticmethod
    def normalize_tweet(tweet,
                        quotation_marks=True,
                        unicode=True,
                        numbers=True,
                        urls=True,
                        emojis=True,
                        hashtags=True,
                        accents=True,
                        punctuation=True,
                        whitespace=True):
        if quotation_marks: tweet = normalize_quotation_marks(tweet)
        if unicode: tweet = normalize_unicode(tweet)
        if numbers: tweet = replace_numbers(tweet, replace_with='NUMBER')
        if urls: tweet = replace_urls(tweet, replace_with='')
        if emojis: tweet = replace_emojis(tweet, replace_with='EMOJI')
        if hashtags: tweet = replace_hashtags(tweet)
        if accents: tweet = remove_accents(tweet)
        if punctuation: tweet = remove_punctuation(tweet, marks='.,?\/()[];:!*+-*="\'▶•◦⁃∞')  # specify marks otherwise @ will also be removed!
        if whitespace:
            tweet = normalize_whitespace(tweet)
            tweet = ' '.join(tweet.split())  # remove excess whitespace

        return tweet

    def lemmatize(self, tweet, keep_hashtag=True):
        # Lemmatize
        if keep_hashtag: tweet = re.sub(r'#(\w+)', r'zzzplaceholderzzz\1', tweet)  # lemmatizer splits #xxx into #, xxx
        doc = self.nlp(tweet)
        doc = [t.lemma_ for t in doc]  # todo: if len(t)>1 ex: 't schip -> t, schip
        tweet = ' '.join(doc)
        if keep_hashtag: tweet = re.sub(r'zzzplaceholderzzz', r'#', tweet)
        return tweet

    def make_bigrams(self):
        t0 = datetime.now()
        corpus = read_text(self.corpora_path + self.corpus_name + '.txt', lines=True)
        print('read corpus:', datetime.now() - t0)
        tweets = [tweet.split() for tweet in corpus]

        print('start making ngrams')
        t1 = datetime.now()
        bigrams = Phrases(tweets, min_count=500, threshold=10, delimiter=b' ', common_terms=list(STOP_WORDS))
        trigrams = Phrases(bigrams[tweets], min_count=100, threshold=8, delimiter=b' ')
        quadgrams = Phrases(trigrams[bigrams[tweets]], min_count=50, threshold=5, delimiter=b' ')
        bigrams.save(self.corpora_path + self.corpus_name + '_bigrams.pkl')
        trigrams.save(self.corpora_path + self.corpus_name + '_trigrams.pkl')
        quadgrams.save(self.corpora_path + self.corpus_name + '_quadgrams.pkl')
        print('ngrams ok and saved:', datetime.now() - t1)

        bigrams_ = []
        trigrams_ = []
        quadgrams_ = []
        xxx = []
        for sent in tweets:
            bigrams_ += [b for b in bigrams[sent] if b.count(' ') == 1]
            trigrams_ += [t for t in trigrams[bigrams[sent]] if t.count(' ') == 2]
            quadgrams_ += [t for t in quadgrams[trigrams[bigrams[sent]]] if t.count(' ') == 3]
            xxx.append([t for t in quadgrams[trigrams[bigrams[sent]]]])
        bigrams_ = sorted(set(bigrams_))
        trigrams_ = sorted(set(trigrams_))
        quadgrams_ = sorted(set(quadgrams_))
        print('-' * 200)
        print(bigrams_)
        print(trigrams_)

        print(quadgrams_)
        print('-' * 200)
        print('-' * 200)
        dictionary = Dictionary(xxx)
        dictionary.save(self.corpora_path + self.corpus_name + '_dictionary.pkl')
        print(len(bigrams_), len(trigrams_), len(quadgrams_))
        print(dictionary)
        print('total time:', datetime.now() - t0)
        # print(dictionary.token2id)

    def tokenize_tweets(self, username, begin_date=None, end_date=None):
        t0 = datetime.now()

        t00 = datetime.now()
        tweets_df = get_tweets(username, begin_date, end_date)
        print('Loading took:', datetime.now() - t00)

        t1 = datetime.now()
        tweets = ''
        for _, row in tweets_df.iterrows():
            tweets += row['tweet']
        print(f'Loop took:', datetime.now() - t1)

        # t2 = datetime.now()
        # doc = self.nlp(tweets)
        # print('Spacy took', datetime.now() - t2)
        #
        print('len tweets', len(tweets))
        from gensim.models import Phrases
        from gensim.models.phrases import Phraser
        from gensim.summarization.textcleaner import get_sentences
        t1 = datetime.now()
        sentenses = get_sentences(tweets)
        print("Gensim get_sentenses took", datetime.now() - t1)
        token_ = [doc.split(" ") for doc in sentenses]

        bigram = Phrases(token_, min_count=3, threshold=10, delimiter=b'_', common_terms=list(STOP_WORDS) + ['ik'])
        print('===========================>', bigram)
        bigram_phraser = Phraser(bigram)
        bigram_token = []
        for sent in token_:
            bigram_token.append(bigram_phraser[sent])
        print(bigram_token)
        bigram_phraser.save('bi.txt')

        trigram_phraser = Phrases(bigram[token_], min_count=2, threshold=5, delimiter=b'_', common_terms=list(STOP_WORDS))
        trigram_token = []
        for sent in token_:
            trigram_token.append(trigram_phraser[sent])
        print(trigram_token)
        trigram_phraser.save('tri.txt')

        return

    def tokenize(self):
        t1 = RobertaTokenizer.from_pretrained("pdelobelle/robBERT-base")
        # t1.save_vocabulary('/media/Development/Twitter_Analytics/src/')
        # t1.save_pretrained('/media/Development/Twitter_Analytics/src/')
        t2 = BertTokenizer.from_pretrained("wietsedv/bert-base-dutch-cased")
        # t2.save_vocabulary('/media/Development/Twitter_Analytics/src/')
        # t2.save_pretrained('/media/Development/Twitter_Analytics/src/')

        # t3 = RobertaTokenizer.from_pretrained('pdelobelle/robBERT-dutch-books')
        x = t1.encode("De tweede poging: nog een test van de tokenizer met nummers.", add_special_tokens=False)
        y = t2.encode("De tweede poging: nog een test van de tokenizer met nummers.", add_special_tokens=False)
        # z = t3.encode("De tweede poging: nog een test van de tokenizer met nummers.", add_special_tokens=False)
        # [2739, 271, 298, 2161, 417]
        print(x)
        print(y)
        # print(z)
        print(t1.vocab_size)
        print(t2.vocab_size)
        # print(t3.vocab_size)


if __name__ == '__main__':
    pass
    tok = TweetCorpus()
    # tok.make_users_copus(sample=5)
    # tok.make_big_corpus()
    # tok.make_bigrams()
    tok.tokenize()
