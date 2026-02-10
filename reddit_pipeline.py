import pendulum
from airflow.sdk import dag, task
import praw
import json
from typing import List, Dict


@dag(
    dag_id="reddit_data_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["reddit", "data-extraction"],
    description="Pipeline to extract data from Reddit and process it",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
)
def reddit_data_pipeline():
    """
    Reddit data extraction pipeline that fetches posts from specified subreddits,
    processes the data, and prepares it for storage or further analysis.
    """

    @task
    def extract_reddit_posts(subreddit_name: str = "python", limit: int = 100):
        """
        Extract posts from a specified subreddit using PRAW.
        
        Note: You need to set up Reddit API credentials in Airflow connections
        or environment variables:
        - REDDIT_CLIENT_ID
        - REDDIT_CLIENT_SECRET
        - REDDIT_USER_AGENT
        """
        import os
        
        try:
            print(f"Extracting posts from r/{subreddit_name}...")
            
            # Initialize Reddit API client
            reddit = praw.Reddit(
                client_id=os.getenv("REDDIT_CLIENT_ID", "your_client_id"),
                client_secret=os.getenv("REDDIT_CLIENT_SECRET", "your_client_secret"),
                user_agent=os.getenv("REDDIT_USER_AGENT", "airflow:reddit_pipeline:v1.0.0")
            )
            
            # Fetch posts from subreddit
            subreddit = reddit.subreddit(subreddit_name)
            posts = []
            
            for post in subreddit.hot(limit=limit):
                post_data = {
                    "id": post.id,
                    "title": post.title,
                    "author": str(post.author) if post.author else "[deleted]",
                    "score": post.score,
                    "upvote_ratio": post.upvote_ratio,
                    "num_comments": post.num_comments,
                    "created_utc": post.created_utc,
                    "url": post.url,
                    "selftext": post.selftext[:500] if post.selftext else "",  # Limit text length
                    "subreddit": subreddit_name,
                    "is_self": post.is_self,
                    "link_flair_text": post.link_flair_text,
                }
                posts.append(post_data)
            
            print(f"Successfully extracted {len(posts)} posts from r/{subreddit_name}")
            
            return {
                "subreddit": subreddit_name,
                "extraction_time": pendulum.now("UTC").to_iso8601_string(),
                "posts": posts,
                "total_posts": len(posts)
            }
            
        except Exception as e:
            print(f"Error extracting Reddit posts: {str(e)}")
            raise

    @task
    def transform_reddit_data(raw_data: Dict):
        """
        Transform and enrich the extracted Reddit data.
        """
        try:
            print("Transforming Reddit data...")
            
            posts = raw_data.get("posts", [])
            transformed_posts = []
            
            for post in posts:
                # Calculate engagement score
                engagement_score = (
                    post["score"] * 0.5 +
                    post["num_comments"] * 2 +
                    post["upvote_ratio"] * 100
                )
                
                # Convert timestamp to readable date
                created_date = pendulum.from_timestamp(post["created_utc"], tz="UTC")
                
                transformed_post = {
                    **post,
                    "engagement_score": round(engagement_score, 2),
                    "created_date": created_date.to_iso8601_string(),
                    "created_date_formatted": created_date.format("YYYY-MM-DD HH:mm:ss"),
                    "has_text": len(post["selftext"]) > 0,
                    "title_length": len(post["title"]),
                    "age_hours": (pendulum.now("UTC") - created_date).in_hours()
                }
                
                transformed_posts.append(transformed_post)
            
            # Sort by engagement score
            transformed_posts.sort(key=lambda x: x["engagement_score"], reverse=True)
            
            # Calculate statistics
            stats = {
                "total_posts": len(transformed_posts),
                "avg_score": sum(p["score"] for p in transformed_posts) / len(transformed_posts) if transformed_posts else 0,
                "avg_comments": sum(p["num_comments"] for p in transformed_posts) / len(transformed_posts) if transformed_posts else 0,
                "avg_engagement": sum(p["engagement_score"] for p in transformed_posts) / len(transformed_posts) if transformed_posts else 0,
                "text_posts": sum(1 for p in transformed_posts if p["has_text"]),
                "link_posts": sum(1 for p in transformed_posts if not p["has_text"])
            }
            
            result = {
                "subreddit": raw_data["subreddit"],
                "extraction_time": raw_data["extraction_time"],
                "transformation_time": pendulum.now("UTC").to_iso8601_string(),
                "posts": transformed_posts,
                "statistics": stats
            }
            
            print(f"Transformed {len(transformed_posts)} posts")
            print(f"Statistics: {json.dumps(stats, indent=2)}")
            
            return result
            
        except Exception as e:
            print(f"Error transforming Reddit data: {str(e)}")
            raise

    @task
    def load_reddit_data(transformed_data: Dict):
        """
        Load the transformed Reddit data to a destination.
        This is a placeholder - adapt to your storage solution (S3, database, etc.)
        """
        try:
            print("Loading Reddit data...")
            
            # Example: Save to JSON file (in production, use proper storage)
            output_filename = f"reddit_{transformed_data['subreddit']}_{pendulum.now('UTC').format('YYYY-MM-DD')}.json"
            
            # In a real scenario, you would:
            # - Save to S3/GCS
            # - Insert into a database
            # - Send to a data warehouse
            # - Push to Elasticsearch
            
            load_summary = {
                "subreddit": transformed_data["subreddit"],
                "posts_loaded": len(transformed_data["posts"]),
                "load_time": pendulum.now("UTC").to_iso8601_string(),
                "output_file": output_filename,
                "statistics": transformed_data["statistics"],
                "status": "success"
            }
            
            print(f"Successfully prepared {load_summary['posts_loaded']} posts for loading")
            print(f"Output file: {output_filename}")
            
            return load_summary
            
        except Exception as e:
            print(f"Error loading Reddit data: {str(e)}")
            raise

    @task
    def generate_report(load_summary: Dict):
        """
        Generate a summary report of the pipeline execution.
        """
        try:
            stats = load_summary["statistics"]
            
            report = f"""
            ╔══════════════════════════════════════════════════════════╗
            ║           Reddit Data Pipeline - Execution Report        ║
            ╚══════════════════════════════════════════════════════════╝
            
            Subreddit: r/{load_summary['subreddit']}
            Execution Time: {load_summary['load_time']}
            Status: {load_summary['status']}
            
            📊 STATISTICS
            ─────────────────────────────────────────────────────────
            Total Posts Processed: {stats['total_posts']}
            Text Posts: {stats['text_posts']}
            Link Posts: {stats['link_posts']}
            
            Average Score: {stats['avg_score']:.2f}
            Average Comments: {stats['avg_comments']:.2f}
            Average Engagement Score: {stats['avg_engagement']:.2f}
            
            Output File: {load_summary['output_file']}
            ─────────────────────────────────────────────────────────
            """
            
            print(report)
            
            return {
                "report": report,
                "execution_time": load_summary['load_time'],
                "status": "completed"
            }
            
        except Exception as e:
            print(f"Error generating report: {str(e)}")
            raise

    # Define task dependencies
    reddit_data = extract_reddit_posts(subreddit_name="python", limit=100)
    transformed = transform_reddit_data(reddit_data)
    loaded = load_reddit_data(transformed)
    report = generate_report(loaded)


# Instantiate the DAG
reddit_data_pipeline()
