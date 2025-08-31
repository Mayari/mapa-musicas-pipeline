name: monthly-refresh

on:
  workflow_dispatch: {}
  schedule:
    - cron: "0 8 1 * *"   # 08:00 UTC on day 1 (~02:00 America/Mexico_City)

jobs:
  refresh:
    runs-on: ubuntu-22.04
    permissions:
      contents: read

    steps:
      - name: Checkout data repo (this repo)
        uses: actions/checkout@v4

      - name: Checkout pipeline repo (public)
        uses: actions/checkout@v4
        with:
          repository: Mayari/mapa-musicas-pipeline    # ← change ONLY if your owner isn't "Mayari"
          token: ${{ secrets.PIPELINE_REPO_TOKEN }}
          path: pipeline_repo

      - name: Verify checkout & tree
        run: |
          echo "::group::pwd & ls"
          pwd && ls -la
          echo "::endgroup::"
          echo "::group::tree pipeline_repo"
          ls -la pipeline_repo || true
          ls -la pipeline_repo/pipeline || true
          ls -la pipeline_repo/data || true
          echo "::endgroup::"

      - name: Verify posters under carteleras
        run: |
          echo "::group::tree carteleras"
          ls -la carteleras || true
          echo "::endgroup::"
          echo "::group::find carteleras"
          find carteleras -maxdepth 3 -type f -print | sed -n '1,100p' || true
          echo "::endgroup::"

      - name: Install system deps
        run: |
          sudo apt-get update -y
          sudo apt-get install -y libcurl4-openssl-dev libssl-dev libxml2-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev libharfbuzz-dev libfribidi-dev libfontconfig1-dev

      - name: Set up R
        uses: r-lib/actions/setup-r@v2
        with:
          r-version: '4.4.1'
          use-public-rspm: true

      - name: Install & cache R packages
        uses: r-lib/actions/setup-r-dependencies@v2
        with:
          extra-packages: |
            any::tidyverse
            any::lubridate
            any::jsonlite
            any::readr
            any::stringr
            any::janitor
            any::httr2
            any::glue
            any::googleLanguageR
          cache-version: 1

      - name: Configure optional secrets
        run: |
          if [ -n "${{ secrets.GCP_SERVICE_ACCOUNT_JSON }}" ]; then
            echo '${{ secrets.GCP_SERVICE_ACCOUNT_JSON }}' > gcp.json
            echo "GOOGLE_APPLICATION_CREDENTIALS=$PWD/gcp.json" >> $GITHUB_ENV
            echo "Configured GCP credentials."
          else
            echo "GCP_SERVICE_ACCOUNT_JSON not set; skipping Vision."
          fi

          if [ -n "${{ secrets.OPENAI_API_KEY }}" ]; then
            echo "OPENAI_API_KEY=${{ secrets.OPENAI_API_KEY }}" >> $GITHUB_ENV
            echo "Configured OpenAI key."
          else
            echo "OPENAI_API_KEY not set; skipping OpenAI extractor."
          fi

          if [ -n "${{ secrets.OPENAI_MODEL }}" ]; then
            echo "OPENAI_MODEL=${{ secrets.OPENAI_MODEL }}" >> $GITHUB_ENV
            echo "Using OpenAI model: ${{ secrets.OPENAI_MODEL }}"
          fi

          if [ -n "${{ secrets.OPENAI_THROTTLE_SEC }}" ]; then
            echo "OPENAI_THROTTLE_SEC=${{ secrets.OPENAI_THROTTLE_SEC }}" >> $GITHUB_ENV
            echo "Using OpenAI throttle: ${{ secrets.OPENAI_THROTTLE_SEC }}s"
          fi

      - name: Run monthly pipeline (from pipeline repo)
        run: |
          set -xe
          cd pipeline_repo
          test -f pipeline/run_monthly.R
          Rscript pipeline/run_monthly.R \
            --images_dir ../carteleras \
            --venues_path data/venues.csv \
            --out_dir data \
            --agg_dir data/aggregations

      - name: List pipeline outputs
        run: |
          ls -la pipeline_repo/data || true
          [ -f pipeline_repo/data/performances_monthly.csv ] && head -n 40 pipeline_repo/data/performances_monthly.csv || echo "(no performances_monthly.csv)"

      - name: Commit & push updates to pipeline repo
        run: |
          cd pipeline_repo
          git config user.name "miss-data-bot"
          git config user.email "actions@users.noreply.github.com"
          git add data/performances_monthly.csv data/aggregations/*.csv || true
          git commit -m "Monthly data refresh [skip ci]" || echo "No changes"
          git push origin HEAD:main
