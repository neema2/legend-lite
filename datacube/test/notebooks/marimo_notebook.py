import marimo

__generated_with = "0.25.1"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    import pandas as pd

    import legend_lite as ll
    return ll, mo, pd


@app.cell
def _(mo):
    rows = mo.ui.slider(1, 4, value=4, label="rows")
    rows
    return (rows,)


@app.cell
def _(pd, rows):
    df = pd.DataFrame({"desk": ["FX", "EQ", "RATES", "FX"], "qty": [10.5, 20.0, 1.0, 7.0]}).head(rows.value)
    return (df,)


@app.cell
def _(df, ll):
    ll.show(df)
    return


if __name__ == "__main__":
    app.run()
