from flask import Flask, request, jsonify
from flask_cors import CORS
from rdflib import Graph, Literal
from fuzzywuzzy import fuzz
import re




g = Graph()

app = Flask(__name__)## Create a Flask application instance, __name__ is the name of the current module.
CORS(app)  ## Staring CORS ,and it will allow front end to visit api
g.parse("movie_ontology_rdf.ttl", format="turtle")
print(f"Loaded {len(g)} triples")## parse the rdf file(knowledge base)and print triple's length.

# Cache for actor list
_actors_cache = None## Underscores means private variables and  it for store all the actor list

# ============================================
# Helper functions
# ============================================
def get_all_actors():
    """Get list of all actor names (with caching)"""
    global _actors_cache
    if _actors_cache is None: ## check is the cache empty ?
        query = """
        PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
        
        SELECT DISTINCT ?actorName
        WHERE {
            ?actor a :Actor_Actress .
            ?actor :name ?actorName .
        }
        """
        results = g.query(query) ##Execute SPARQL query
        _actors_cache = [str(row.actorName) for row in results]##Iterate through the result,converting each actorName to a string
        print(f"Cached {len(_actors_cache)} actors")
    return _actors_cache

def find_similar_actors(input_name, threshold=0.5):## recieve name and similar threshold euqals to 0.5
    """Find similar actors using fuzzywuzzy"""
    actors = get_all_actors()
    input_lower = input_name.lower().strip()## covert capital letter to lower and delete space ' 'at head and tail
    
    if len(input_lower) < 2:## at least input 2 letter ,if not more than 2 letter,will return empty list
        return []
    
    matches = []## initalization the empty list to store match outcome
    for actor in actors:## travesal all actor
        actor_lower = actor.lower()
        # fuzzywuzzy returns 0-100, divide by 100 to get 0-1
        ratio = fuzz.ratio(input_lower, actor_lower) / 100## compute the similar
        
        if ratio > threshold:
            matches.append((actor, ratio))##If satisfy the condition,add (actor, ratio)array to list
    
    # Sort by similarity score
    matches.sort(key=lambda x: x[1], reverse=True)## sort by similarity score
    return matches[:5]

def format_movie_result(row):
    """Format SPARQL result row to dictionary"""
    movie = {
        'title': str(row.title) if hasattr(row, 'title') else None,
        'year': int(row.year) if hasattr(row, 'year') and row.year else None,
        'rating': float(row.rating) if hasattr(row, 'rating') and row.rating else None,
        'actorName': str(row.actorName) if hasattr(row, 'actorName') else None,
        'directorName': str(row.directorName) if hasattr(row, 'directorName') else None,
        'genreName': str(row.genreName) if hasattr(row, 'genreName') else None,
        'studioName': str(row.studioName) if hasattr(row, 'studioName') else None,
    }## check row object has attribute or not ..eg :"if hasattr(row, 'title') else None "has 'title' attribute or not
    return {k: v for k, v in movie.items() if v is not None}

# ============================================
# API Routes
# ============================================

@app.route('/', methods=['GET'])## only recieve the 'GET'request
def home():
    """API home page"""
    return jsonify({
        'name': 'Movie Search Engine API',
        'version': '1.0',
        'endpoints': {
            '/search?q=<query>': 'Universal search across all fields',
            '/search/actor?name=<name>': 'Search movies by actor',
            '/search/director?name=<name>': 'Search movies by director',
            '/search/genre?name=<name>': 'Search movies by genre',
            '/search/studio?name=<name>': 'Search movies by studio',
            '/movie/<id>': 'Get movie details by ID',
            '/stats': 'Get database statistics'
        }## convert dictionary to json response and return api information
    })

@app.route('/search', methods=['GET'])## define a search router
def universal_search():
    """Universal search across all fields"""
    query_text = request.args.get('q', '').strip()## get url query parameter 
    
    if not query_text or len(query_text) < 2:
        return jsonify({'error': 'Please provide at least 2 characters'}), 400## error request in search frame
    
    # Clean input
    clean_input = re.sub(r'\s+', ' ', query_text.lower().strip())## match the empty characters and replace single space
    keywords = [k for k in clean_input.split() if len(k) > 2]
    
    if not keywords:
        return jsonify({'error': 'No valid keywords and at least 2 letters'}), 400
    
    # Build conditions for each keyword
    keyword_conditions = []
    for kw in keywords:
        keyword_conditions.append(f'CONTAINS(LCASE(?matchText), "{kw}")')
    ## check it has lcase match text or not 
    filter_text = ' && '.join(keyword_conditions)
    ## join():use '&&'connect all conditions
    query = f"""
    PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
    
    SELECT DISTINCT ?movie ?title ?year ?rating ?matchField ?matchText
    WHERE {{
        {{
            # Search in titles
            ?movie a :Movie .
            ?movie :title ?title .
            OPTIONAL {{ ?movie :releaseYear ?year . }}
            OPTIONAL {{ ?movie :rating ?rating . }}
            BIND(?title AS ?matchText)
            BIND("Title" AS ?matchField)
            FILTER({filter_text})
        }}
        UNION
        {{
            # Search in actor names
            ?movie a :Movie .
            ?movie :title ?title .
            ?actor :actedIn ?movie .
            ?actor :name ?actorName .
            OPTIONAL {{ ?movie :releaseYear ?year . }}
            OPTIONAL {{ ?movie :rating ?rating . }}
            BIND(?actorName AS ?matchText)
            BIND("Actor" AS ?matchField)
            FILTER({filter_text})
        }}
        UNION
        {{
            # Search in director names
            ?movie a :Movie .
            ?movie :title ?title .
            ?movie :directedBy ?director .
            ?director :name ?directorName .
            OPTIONAL {{ ?movie :releaseYear ?year . }}
            OPTIONAL {{ ?movie :rating ?rating . }}
            BIND(?directorName AS ?matchText)
            BIND("Director" AS ?matchField)
            FILTER({filter_text})
        }}
        UNION
        {{
            # Search in genres
            ?movie a :Movie .
            ?movie :title ?title .
            ?movie :hasGenre ?genre .
            ?genre :GenreName ?genreName .
            OPTIONAL {{ ?movie :releaseYear ?year . }}
            OPTIONAL {{ ?movie :rating ?rating . }}
            BIND(?genreName AS ?matchText)
            BIND("Genre" AS ?matchField)
            FILTER({filter_text})
        }}
        UNION
        {{
            # Search in studios
            ?movie a :Movie .
            ?movie :title ?title .
            ?movie :producedBy ?studio .
            ?studio :CompanyName ?studioName .
            OPTIONAL {{ ?movie :releaseYear ?year . }}
            OPTIONAL {{ ?movie :rating ?rating . }}
            BIND(?studioName AS ?matchText)
            BIND("Studio" AS ?matchField)
            FILTER({filter_text})
        }}
    }}
    ORDER BY DESC(?rating)
    LIMIT 50
    """
    
    results = g.query(query)## execute query
    
    movies = []
    for row in results:
        movies.append({
            'title': str(row.title),
            'year': int(row.year) if row.year else None,
            'rating': float(row.rating) if row.rating else None,
            'matchField': str(row.matchField),
            'matchText': str(row.matchText)
        })
    ## traversal the result set ,create movie list
    # Remove duplicates (same movie might appear from different matches)
    seen = set()
    unique_movies = []
    for movie in movies:
        if movie['title'] not in seen:
            seen.add(movie['title'])
            unique_movies.append(movie)
    
    return jsonify({
        'query': query_text,
        'total': len(unique_movies),
        'results': unique_movies
    })

@app.route('/search/actor', methods=['GET'])## a router about search by actor 
def search_by_actor():
    """Search movies by actor name (with fuzzy matching)"""
    actor_name = request.args.get('name', '').strip()## get actor name
    
    if not actor_name or len(actor_name) < 2:
        return jsonify({'error': 'Please provide at least 2 characters'}), 400
    
    # Clean input
    clean_input = re.sub(r'\s+', ' ', actor_name.lower().strip())
    keywords = [k for k in clean_input.split() if len(k) > 2]
    
    # Try exact keyword matching first
    if keywords:
        conditions = [f'CONTAINS(LCASE(?actorName), "{k}")' for k in keywords]
        filter_text = ' && '.join(conditions)
        
        query = f"""
        PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
        
        SELECT DISTINCT ?movie ?title ?year ?rating ?actorName
        WHERE {{
            ?actor a :Actor_Actress .
            ?actor :name ?actorName .
            FILTER({filter_text})
            
            ?actor :actedIn ?movie .
            ?movie :title ?title .
            OPTIONAL {{ ?movie :releaseYear ?year . }}
            OPTIONAL {{ ?movie :rating ?rating . }}
        }}
        ORDER BY DESC(?year)
        LIMIT 50
        """
        
        results = list(g.query(query))
        
        if results:
            movies = []
            for row in results:
                movies.append({
                    'title': str(row.title),
                    'year': int(row.year) if row.year else None,
                    'rating': float(row.rating) if row.rating else None,
                    'actorName': str(row.actorName)
                })
            
            return jsonify({
                'query': actor_name,
                'matchType': 'exact',
                'total': len(movies),
                'results': movies
            })
    
    # If no exact matches, try fuzzy search
    similar = find_similar_actors(actor_name)
    
    if not similar:
        return jsonify({
            'query': actor_name,
            'matchType': 'none',
            'total': 0,
            'results': [],
            'suggestions': []
        })
    
    # Use best match
    best_match = similar[0][0]## get the similarest actor name
    suggestions = [{'name': a, 'similarity': s} for a, s in similar]
    
    query = f"""
    PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
    
    SELECT DISTINCT ?movie ?title ?year ?rating ?actorName
    WHERE {{
        ?actor a :Actor_Actress .
        ?actor :name ?actorName .
        FILTER(CONTAINS(LCASE(?actorName), "{best_match.lower()}"))
        
        ?actor :actedIn ?movie .
        ?movie :title ?title .
        OPTIONAL {{ ?movie :releaseYear ?year . }}
        OPTIONAL {{ ?movie :rating ?rating . }}
    }}
    ORDER BY DESC(?year)
    LIMIT 50
    """
    ## use the best match to research
    results = g.query(query)
    
    movies = []
    for row in results:
        movies.append({
            'title': str(row.title),
            'year': int(row.year) if row.year else None,
            'rating': float(row.rating) if row.rating else None,
            'actorName': str(row.actorName)
        })
    
    return jsonify({
        'query': actor_name,
        'matchType': 'fuzzy',
        'matchedActor': best_match,
        'total': len(movies),
        'results': movies,
        'suggestions': suggestions
    })
''''''
@app.route('/search/director', methods=['GET'])
def search_by_director():
    """Search movies by director name"""
    director_name = request.args.get('name', '').strip()
    
    if not director_name or len(director_name) < 2:
        return jsonify({'error': 'Please provide at least 2 characters'}), 400
    
    # Clean input
    clean_input = re.sub(r'\s+', ' ', director_name.lower().strip())
    keywords = [k for k in clean_input.split() if len(k) > 2]
    
    if not keywords:
        return jsonify({'error': 'No valid keywords at least 2 letters'}), 400
    
    conditions = [f'CONTAINS(LCASE(?directorName), "{k}")' for k in keywords]
    filter_text = ' && '.join(conditions)
    
    query = f"""
    PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
    
    SELECT DISTINCT ?movie ?title ?year ?rating ?directorName
    WHERE {{
        ?director a :Director .
        ?director :name ?directorName .
        FILTER({filter_text})
        
        ?movie :directedBy ?director .
        ?movie :title ?title .
        OPTIONAL {{ ?movie :releaseYear ?year . }}
        OPTIONAL {{ ?movie :rating ?rating . }}
    }}
    ORDER BY DESC(?year)
    LIMIT 50
    """
    
    results = g.query(query)
    
    movies = []
    for row in results:
        movies.append({
            'title': str(row.title),
            'year': int(row.year) if row.year else None,
            'rating': float(row.rating) if row.rating else None,
            'directorName': str(row.directorName)
        })
    
    return jsonify({
        'query': director_name,
        'total': len(movies),
        'results': movies
    })

@app.route('/search/genre', methods=['GET'])
def search_by_genre():
    """Search movies by genre"""
    genre_name = request.args.get('name', '').strip()
    
    if not genre_name or len(genre_name) < 2:
        return jsonify({'error': 'Please provide at least 2 characters'}), 400
    
    query = f"""
    PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
    
    SELECT DISTINCT ?movie ?title ?year ?rating ?genreName
    WHERE {{
        ?genre a :Genre .
        ?genre :GenreName ?genreName .
        FILTER(CONTAINS(LCASE(?genreName), LCASE("{genre_name}")))
        
        ?movie :hasGenre ?genre .
        ?movie :title ?title .
        OPTIONAL {{ ?movie :releaseYear ?year . }}
        OPTIONAL {{ ?movie :rating ?rating . }}
    }}
    ORDER BY DESC(?rating)
    LIMIT 50
    """
    
    results = g.query(query)
    
    movies = []
    for row in results:
        movies.append({
            'title': str(row.title),
            'year': int(row.year) if row.year else None,
            'rating': float(row.rating) if row.rating else None,
            'genreName': str(row.genreName)
        })
    
    return jsonify({
        'query': genre_name,
        'total': len(movies),
        'results': movies
    })

@app.route('/search/studio', methods=['GET'])
def search_by_studio():
    """Search movies by production studio"""
    studio_name = request.args.get('name', '').strip()
    
    if not studio_name or len(studio_name) < 2:
        return jsonify({'error': 'Please provide at least 2 characters'}), 400
    
    query = f"""
    PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
    
    SELECT DISTINCT ?movie ?title ?year ?rating ?studioName
    WHERE {{
        ?studio a :Company .
        ?studio :CompanyName ?studioName .
        FILTER(CONTAINS(LCASE(?studioName), LCASE("{studio_name}")))
        
        ?movie :producedBy ?studio .
        ?movie :title ?title .
        OPTIONAL {{ ?movie :releaseYear ?year . }}
        OPTIONAL {{ ?movie :rating ?rating . }}
    }}
    ORDER BY DESC(?rating)
    LIMIT 50
    """
    
    results = g.query(query)
    
    movies = []
    for row in results:
        movies.append({
            'title': str(row.title),
            'year': int(row.year) if row.year else None,
            'rating': float(row.rating) if row.rating else None,
            'studioName': str(row.studioName)
        })
    
    return jsonify({
        'query': studio_name,
        'total': len(movies),
        'results': movies
    })

@app.route('/movie/<string:movie_id>', methods=['GET'])
def get_movie_details(movie_id):
    """Get detailed information about a specific movie"""
    
    query = f"""
    PREFIX : <http://www.semanticweb.org/legion/ontologies/2026/0/untitled-ontology-5/>
    
    SELECT ?property ?value
    WHERE {{
        ?movie a :Movie .
        ?movie :title ?title .
        FILTER(CONTAINS(LCASE(?title), LCASE("{movie_id}")))
        ?movie ?property ?value .
    }}
    """
    
    results = g.query(query)
    
    details = {}
    for row in results:
        prop = str(row.property).split('#')[-1] if '#' in str(row.property) else str(row.property)
        val = str(row.value)
        
        if prop not in details:
            details[prop] = val
        else:
            if not isinstance(details[prop], list):
                details[prop] = [details[prop]]
            details[prop].append(val)
    
    if not details:
        return jsonify({'error': 'Movie not found'}), 404
    
    return jsonify(details)

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    
    queries = {
        'movies': "SELECT (COUNT(?m) AS ?count) WHERE { ?m a :Movie . }",
        'actors': "SELECT (COUNT(?a) AS ?count) WHERE { ?a a :Actor_Actress . }",
        'directors': "SELECT (COUNT(?d) AS ?count) WHERE { ?d a :Director . }",
        'genres': "SELECT (COUNT(?g) AS ?count) WHERE { ?g a :Genre . }",
        'companies': "SELECT (COUNT(?c) AS ?count) WHERE { ?c a :Company . }",
        'countries': "SELECT (COUNT(?c) AS ?count) WHERE { ?c a :Country . }"
    }
    
    stats = {}
    for name, q in queries.items():
        result = list(g.query(q))
        stats[name] = int(result[0][0]) if result else 0
    
    stats['total_triples'] = len(g)
    
    return jsonify(stats)

# ============================================
# Run the API
# ============================================
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)