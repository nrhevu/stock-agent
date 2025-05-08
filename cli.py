#!/usr/bin/env python3
import argparse
import json
import logging
import sys
from typing import Dict, Any

from core.executor import execute_query, execute_intent
from core.agents import IntentRecognitionAgent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def format_output(result: Dict[str, Any]) -> str:
    """Format the result for CLI output"""
    if "error" in result:
        return f"ERROR: {result['error']}"
    
    output = []
    
    # Add query info
    if "query" in result:
        output.append(f"Query: {result['query']}")
    
    # Add intent info if available
    if "intent" in result:
        output.append(f"Detected Intent: {result['intent']}")
    
    # Add company info
    if "company_name" in result and "ticker" in result:
        output.append(f"Company: {result['company_name']} ({result['ticker']})")
    
    # Add news data if available
    if "news_data" in result and result["news_data"]:
        output.append("\n=== NEWS DATA ===")
        output.append(result["news_data"])
    
    # Add technical analysis if available
    if "technical_analysis" in result and result["technical_analysis"]:
        output.append("\n=== TECHNICAL ANALYSIS ===")
        output.append(result["technical_analysis"])
    
    # Add prediction if available
    if "prediction" in result and result["prediction"]:
        output.append("\n=== PREDICTION ===")
        output.append(result["prediction"])
    
    return "\n".join(output)

def main():
    parser = argparse.ArgumentParser(description="Stock Agent CLI")
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Query command
    query_parser = subparsers.add_parser("query", help="Process a natural language query")
    query_parser.add_argument("text", help="The query text")
    query_parser.add_argument("--mode", choices=["intent", "agent", "sequential"], default="intent",
                             help="Execution mode (default: intent)")
    query_parser.add_argument("--news-days", type=int, default=30,
                             help="Number of days to look back for news (default: 30)")
    query_parser.add_argument("--price-days", type=int, default=90,
                             help="Number of days to look back for stock prices (default: 90)")
    
    # Intent command
    intent_parser = subparsers.add_parser("intent", help="Process a query with a specific intent")
    intent_parser.add_argument("text", help="The query text")
    intent_parser.add_argument("--intent", choices=["retrieve_news", "retrieve_stock", "analyze_stock"], required=True,
                              help="The specific intent to execute")
    intent_parser.add_argument("--news-days", type=int, default=30,
                              help="Number of days to look back for news (default: 30)")
    intent_parser.add_argument("--price-days", type=int, default=90,
                              help="Number of days to look back for stock prices (default: 90)")
    
    # Interactive mode
    interactive_parser = subparsers.add_parser("interactive", help="Start interactive mode")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Execute the appropriate command
    if args.command == "query":
        result = execute_query(
            query=args.text,
            mode=args.mode,
            news_days=args.news_days,
            price_days=args.price_days
        )
        print(format_output(result))
        
    elif args.command == "intent":
        result = execute_intent(
            query=args.text,
            intent=args.intent,
            news_days=args.news_days,
            price_days=args.price_days
        )
        print(format_output(result))
        
    elif args.command == "interactive":
        print("=== Stock Agent Interactive Mode ===")
        print("Type 'exit' or 'quit' to exit.")
        print("Type 'help' for available commands.")
        
        while True:
            try:
                user_input = input("\nEnter your query: ")
                
                if user_input.lower() in ["exit", "quit"]:
                    print("Exiting interactive mode.")
                    break
                    
                elif user_input.lower() == "help":
                    print("\nAvailable commands:")
                    print("  exit, quit - Exit interactive mode")
                    print("  help - Show this help message")
                    print("  intent <intent_type> <query> - Execute a specific intent")
                    print("    Intent types: retrieve_news, retrieve_stock, analyze_stock")
                    print("  Any other input will be processed as a natural language query")
                    
                elif user_input.lower().startswith("intent "):
                    parts = user_input.split(" ", 2)
                    if len(parts) < 3:
                        print("ERROR: Intent command requires an intent type and a query.")
                        print("Usage: intent <intent_type> <query>")
                        continue
                        
                    intent_type = parts[1]
                    query_text = parts[2]
                    
                    if intent_type not in ["retrieve_news", "retrieve_stock", "analyze_stock"]:
                        print(f"ERROR: Unknown intent type '{intent_type}'")
                        print("Valid intent types: retrieve_news, retrieve_stock, analyze_stock")
                        continue
                        
                    result = execute_intent(
                        query=query_text,
                        intent=intent_type
                    )
                    print(format_output(result))
                    
                else:
                    # Process as a natural language query
                    result = execute_query(query=user_input)
                    print(format_output(result))
                    
            except KeyboardInterrupt:
                print("\nExiting interactive mode.")
                break
                
            except Exception as e:
                print(f"ERROR: {str(e)}")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
