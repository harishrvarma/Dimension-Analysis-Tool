import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from difflib import SequenceMatcher


class BaseAlgorithm:
    """Base class for matching algorithms"""
    
    def normalize_text(self, text: str) -> str:
        if pd.isna(text):
            return ""
        text = str(text).lower()
        text = re.sub(r'https?://(www\.)?', '', text)
        text = re.sub(r'\.com|\.org|\.net', '', text)
        text = re.sub(r'[^a-z0-9\s\-]', ' ', text)
        return ' '.join(text.split())
    
    def extract_url_path(self, url: str) -> str:
        if pd.isna(url):
            return ""
        path = re.sub(r'https?://[^/]+/', '', str(url))
        path = re.sub(r'\?.*$', '', path)
        return self.normalize_text(path)
    
    def score(self, val1, val2) -> float:
        raise NotImplementedError


class TfidfAlgorithm(BaseAlgorithm):
    """TF-IDF vectorization algorithm"""
    
    def score(self, val1, val2) -> float:
        norm1 = self.normalize_text(str(val1))
        norm2 = self.normalize_text(str(val2))
        
        if not norm1 or not norm2 or len(norm1) < 2 or len(norm2) < 2:
            return 0.0
        
        vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(2, 4), min_df=1)
        try:
            mat = vectorizer.fit_transform([norm1, norm2])
            similarity = cosine_similarity(mat[0:1], mat[1:])[0][0]
            return float(similarity * 100)
        except:
            return 0.0


class SkuAlgorithm(BaseAlgorithm):
    """SKU matching algorithm"""
    
    def extract_sku_components(self, sku: str) -> list:
        sku = self.normalize_text(sku)
        parts = re.split(r'[\-_\s]+', sku)
        return [p for p in parts if p]
    
    def score(self, val1, val2) -> float:
        a = set(self.extract_sku_components(str(val1)))
        b = set(self.extract_sku_components(str(val2)))
        if not a or not b:
            return 0.0
        common = a & b
        weighted = sum(len(c) for c in common)
        total = sum(len(c) for c in a)
        return (weighted / total * 100) if total > 0 else 0.0


class PriceAlgorithm(BaseAlgorithm):
    """Price similarity algorithm with margin configuration"""
    
    def __init__(self, margin=25, margin_diff=10):
        self.margin = margin
        self.margin_diff = margin_diff
    
    def score(self, val1, val2) -> float:
        try:
            price = float(val1)
            comp_price = float(val2)
            if price <= 0 or comp_price <= 0:
                return 0.0
            
            # Calculate margin and diff amounts
            margin_amount = price * (self.margin / 100)
            diff_amount = price * (self.margin_diff / 100)
            
            # Calculate target price and range
            target_price = price + margin_amount
            min_price = target_price - diff_amount
            max_price = target_price + diff_amount
            
            # If competitor price is outside range, return 0
            if comp_price < min_price or comp_price > max_price:
                return 0.0
            
            # Calculate similarity score
            price_range = max_price - min_price
            price_diff = abs(target_price - comp_price)
            
            if price_range == 0:
                return 100.0 if price_diff == 0 else 0.0
            
            diff_percentage = (price_diff / price_range) * 100
            similarity = 100 - diff_percentage
            
            return max(0, min(100, similarity))
        except:
            return 0.0


class UrlAlgorithm(BaseAlgorithm):
    """URL similarity algorithm"""
    
    def score(self, val1, val2) -> float:
        path1 = self.extract_url_path(str(val1))
        path2 = self.extract_url_path(str(val2))
        if not path1 or not path2:
            return 0.0
        return SequenceMatcher(None, path1, path2).ratio() * 100


class CustomAlgorithm(BaseAlgorithm):
    """Custom algorithm combining SKU, URL, and Price"""
    
    def __init__(self, margin=25, margin_diff=10):
        self.sku_algo = SkuAlgorithm()
        self.url_algo = UrlAlgorithm()
        self.price_algo = PriceAlgorithm(margin, margin_diff)
    
    def score_sku(self, val1, val2) -> float:
        return self.sku_algo.score(val1, val2)
    
    def score_url(self, val1, val2) -> float:
        return self.url_algo.score(val1, val2)
    
    def score_price(self, val1, val2) -> float:
        return self.price_algo.score(val1, val2)


class TfidfPriceAlgorithm(BaseAlgorithm):
    """TF-IDF for SKU/URL + Margin-based Price algorithm"""
    
    def __init__(self, margin=25, margin_diff=10):
        self.tfidf_algo = TfidfAlgorithm()
        self.price_algo = PriceAlgorithm(margin, margin_diff)
    
    def score_sku(self, val1, val2) -> float:
        return self.tfidf_algo.score(val1, val2)
    
    def score_url(self, val1, val2) -> float:
        return self.tfidf_algo.score(val1, val2)
    
    def score_price(self, val1, val2) -> float:
        return self.price_algo.score(val1, val2)


class AlgorithmFactory:
    """Factory to create algorithm instances"""
    
    @staticmethod
    def get_algorithm(algo_name: str, price_config=None):
        price_config = price_config or {}
        margin = price_config.get('margin', 25)
        margin_diff = price_config.get('margin_diff', 10)
        
        algorithms = {
            'tfidf': TfidfAlgorithm(),
            'sku': SkuAlgorithm(),
            'price': PriceAlgorithm(margin, margin_diff),
            'url': UrlAlgorithm(),
            'custom': CustomAlgorithm(margin, margin_diff),
            'tfidf_price': TfidfPriceAlgorithm(margin, margin_diff)
        }
        return algorithms.get(algo_name)
