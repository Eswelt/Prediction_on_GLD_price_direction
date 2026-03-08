# BUS 32120 Final Project

This project asks a practical investing question:

**For predicting next-month GLD direction, which signal is more reliable in our sample period: index signals (UUP/VIX) or WSJ headline text?**

## What is in this folder

- `final_project_compared.ipynb`: main notebook (data pipeline, modeling, evaluation, plots)
- `articles.pq`: WSJ headlines dataset
- `final_project.db`: SQLite database built by the notebook
- `final_queries.sql`: SQL script used inside the workflow
- `final_slides.pptx`: current slide deck

## Data window used in the analysis

- Market/news overlap window: **2007-03-01 to 2017-12-31**
- Prediction target: next-month GLD direction (up/down)
- Split: chronological train / validation / test

## How to run

1. Open `final_project_compared.ipynb`.
2. Run cells from top to bottom.
3. The notebook will:
   - load and clean market + headline data,
   - build monthly features and labels,
   - train index-only and text-only logistic models,
   - compare threshold rules (F1 vs MCC),
   - export figures to `slides_assets/`.

## Current takeaway

- We use **MCC** as the main decision metric because it balances TP/TN/FP/FN and is better aligned with directional reliability.
- In this sample, the **text model** is more reliable out-of-sample than the **index-only model** under MCC-based selection.
- F1-based selection is still useful when the goal is “do not miss upside,” but it can reward majority-class behavior.

## Notes

- Results are sample-dependent and should not be treated as a universal trading rule.
- Monthly sample size is limited, so conclusions should be interpreted with caution.
