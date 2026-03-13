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
        """Extract and normalize URL path, focusing on product identifiers"""
        if pd.isna(url):
            return ""
        # Remove protocol and domain
        path = re.sub(r'https?://[^/]+/', '', str(url))
        # Remove query parameters
        path = re.sub(r'\?.*$', '', path)
        # Remove file extensions
        path = re.sub(r'\.(html|htm|php|asp|aspx)$', '', path)
        # Replace hyphens and underscores with spaces
        path = re.sub(r'[-_/]', ' ', path)
        return self.normalize_text(path)
    
    def extract_product_tokens(self, url: str) -> set:
        """Extract meaningful product tokens from URL (SKU, model numbers, etc.)"""
        if pd.isna(url):
            return set()
        
        # Get the path without domain
        path = re.sub(r'https?://[^/]+/', '', str(url))
        path = re.sub(r'\?.*$', '', path)  # Remove query params
        path = re.sub(r'\.(html|htm|php|asp|aspx)$', '', path)  # Remove extensions
        
        # Normalize
        path = path.lower()
        path = re.sub(r'[^a-z0-9\-]', ' ', path)  # Keep alphanumeric and hyphens
        
        # Split into tokens
        tokens = path.split()
        
        # Filter out common path words that aren't product identifiers
        stopwords = {'products', 'product', 'item', 'items', 'furniture', 'pdp', 'p', 
                     'catalog', 'shop', 'buy', 'detail', 'details', 'view', 'page'}
        
        # Keep tokens that are:
        # 1. Longer than 2 characters OR
        # 2. Contain both letters and numbers (likely SKU/model)
        meaningful_tokens = set()
        for token in tokens:
            if token in stopwords:
                continue
            # Keep if it has both letters and numbers (SKU pattern)
            if re.search(r'[a-z]', token) and re.search(r'[0-9]', token):
                meaningful_tokens.add(token)
            # Or if it's a longer token (3+ chars) that's not a common word
            elif len(token) >= 3:
                meaningful_tokens.add(token)
        
        return meaningful_tokens
    
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
    """SKU matching algorithm with semicolon-separated list support"""
    
    def extract_sku_components(self, sku: str) -> list:
        sku = self.normalize_text(sku)
        parts = re.split(r'[\-_\s]+', sku)
        return [p for p in parts if p]
    
    def score(self, val1, val2) -> float:
        """Enhanced SKU matching with semicolon-separated list support"""
        if pd.isna(val1) or pd.isna(val2):
            return 0.0
        
        # Convert to strings and normalize
        str1 = str(val1).strip()
        str2 = str(val2).strip()
        
        # Split by semicolon and clean up each part
        list1 = [item.strip().lower() for item in str1.split(';') if item.strip()]
        list2 = [item.strip().lower() for item in str2.split(';') if item.strip()]
        
        # If either list is empty, fall back to original algorithm
        if not list1 or not list2:
            a = set(self.extract_sku_components(str1))
            b = set(self.extract_sku_components(str2))
            if not a or not b:
                return 0.0
            common = a & b
            weighted = sum(len(c) for c in common)
            total = sum(len(c) for c in a)
            return (weighted / total * 100) if total > 0 else 0.0
        
        # Check for exact matches between any items in the lists
        for item1 in list1:
            for item2 in list2:
                if item1 == item2:
                    return 100.0  # Perfect match found
        
        # If no exact match, try component-based matching on each pair
        max_score = 0.0
        for item1 in list1:
            for item2 in list2:
                a = set(self.extract_sku_components(item1))
                b = set(self.extract_sku_components(item2))
                if a and b:
                    common = a & b
                    weighted = sum(len(c) for c in common)
                    total = sum(len(c) for c in a)
                    score = (weighted / total * 100) if total > 0 else 0.0
                    max_score = max(max_score, score)
        
        return max_score


class PriceAlgorithm(BaseAlgorithm):
    """Price similarity algorithm with separate low and upper margin limits"""
    
    def __init__(self, margin=25, margin_low_limit=10, margin_upper_limit=10):
        self.margin = margin
        self.margin_low_limit = margin_low_limit
        self.margin_upper_limit = margin_upper_limit
    
    def score(self, val1, val2) -> float:
        try:
            price = float(val1)
            comp_price = float(val2)
            if price <= 0 or comp_price <= 0:
                return 0.0
            
            # Calculate margin and limit amounts
            margin_amount = price * (self.margin / 100)
            low_amount = price * (self.margin_low_limit / 100)
            upper_amount = price * (self.margin_upper_limit / 100)
            
            # Calculate target price and asymmetric range
            target_price = price + margin_amount
            min_price = target_price - low_amount
            max_price = target_price + upper_amount
            
            # If competitor price is outside range, return 0
            if comp_price < min_price or comp_price > max_price:
                return 0.0
            
            # Calculate similarity score based on distance from target
            if comp_price <= target_price:
                # Price is below target, use low limit range
                if min_price == target_price:
                    return 100.0 if comp_price == target_price else 0.0
                distance_ratio = abs(target_price - comp_price) / (target_price - min_price)
            else:
                # Price is above target, use upper limit range
                if max_price == target_price:
                    return 100.0 if comp_price == target_price else 0.0
                distance_ratio = abs(comp_price - target_price) / (max_price - target_price)
            
            similarity = (1 - distance_ratio) * 100
            return max(0, min(100, similarity))
        except:
            return 0.0


class UrlAlgorithm(BaseAlgorithm):
    """URL similarity algorithm - URLs are pre-normalized at data loading stage"""
    
    def score(self, val1, val2) -> float:
        # URLs are already normalized, just compare tokens
        if pd.isna(val1) or pd.isna(val2):
            return 0.0
        
        words1 = set(str(val1).split())
        words2 = set(str(val2).split())
        
        if not words1 or not words2:
            return 0.0
        
        # Calculate word overlap
        common = words1 & words2
        total = words1 | words2
        
        return (len(common) / len(total)) * 100 if total else 0


class CustomAlgorithm(BaseAlgorithm):
    """Custom algorithm combining SKU, URL, and Price"""
    
    def __init__(self, margin=25, margin_low_limit=10, margin_upper_limit=10):
        self.sku_algo = SkuAlgorithm()
        self.url_algo = UrlAlgorithm()
        self.price_algo = PriceAlgorithm(margin, margin_low_limit, margin_upper_limit)
    
    def score_sku(self, val1, val2) -> float:
        """Enhanced SKU matching with semicolon-separated list support"""
        if pd.isna(val1) or pd.isna(val2):
            return 0.0
        
        # Convert to strings and normalize
        str1 = str(val1).strip()
        str2 = str(val2).strip()
        
        # Split by semicolon and clean up each part
        list1 = [item.strip().lower() for item in str1.split(';') if item.strip()]
        list2 = [item.strip().lower() for item in str2.split(';') if item.strip()]
        
        # If either list is empty, return 0
        if not list1 or not list2:
            return 0.0
        
        # Check for exact matches between any items in the lists
        for item1 in list1:
            for item2 in list2:
                if item1 == item2:
                    return 100.0  # Perfect match found
        
        # If no exact match, fall back to original SKU algorithm
        fallback_score = self.sku_algo.score(val1, val2)
        return fallback_score
    
    def score_url(self, val1, val2) -> float:
        return self.url_algo.score(val1, val2)
    
    def score_price(self, val1, val2) -> float:
        return self.price_algo.score(val1, val2)


class TfidfPriceAlgorithm(BaseAlgorithm):
    """TF-IDF for SKU/URL + Margin-based Price algorithm"""
    
    def __init__(self, margin=25, margin_low_limit=10, margin_upper_limit=10):
        self.tfidf_algo = TfidfAlgorithm()
        self.price_algo = PriceAlgorithm(margin, margin_low_limit, margin_upper_limit)
    
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
        margin_low_limit = price_config.get('margin_low_limit', 10)
        margin_upper_limit = price_config.get('margin_upper_limit', 10)
        
        algorithms = {
            'tfidf': TfidfAlgorithm(),
            'sku': SkuAlgorithm(),
            'price': PriceAlgorithm(margin, margin_low_limit, margin_upper_limit),
            'url': UrlAlgorithm(),
            'custom': CustomAlgorithm(margin, margin_low_limit, margin_upper_limit),
            'tfidf_price': TfidfPriceAlgorithm(margin, margin_low_limit, margin_upper_limit)
        }
        return algorithms.get(algo_name)
