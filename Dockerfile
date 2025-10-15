# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy all project files
COPY . .

# Install all necessary Python dependencies
# This now includes auth libraries needed by gspread.
RUN pip install --no-cache-dir scrapy scrapy-playwright gspread google-auth-oauthlib google-generativeai

# Install system dependencies required by Playwright browsers
RUN playwright install-deps

# Create and switch to a non-root user for security
RUN useradd -m scrapy_user
USER scrapy_user

# Install the browser itself as the non-root user
RUN playwright install chromium

# Set the default command for the container
CMD ["scrapy", "crawl", "unfccc_events"]



