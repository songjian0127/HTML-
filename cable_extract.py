import os
import csv
import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

DB_FILENAME = "database.db"   # change if your DB is elsewhere
KEYWORD = ""               # searched (case-insensitive) within Cable.Name


def fetch_rows(db_path: str, keyword: str):
    """
    Returns list of tuples:
    (link1_name, link1_manhole, cable_name, cable_length, link2_name, link2_manhole, same_manhole)
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    sql = """
    SELECT
        s1.Name        AS Link1Name,
        s1.MANHOLE     AS Link1Manhole,
        c.Name         AS CableName,
        c.SPAN_LENGTH  AS CableLength,
        s2.Name        AS Link2Name,
        s2.MANHOLE     AS Link2Manhole
    FROM Cable c
    LEFT JOIN SpliceCases s1 ON s1.ID = c.LINK1
    LEFT JOIN SpliceCases s2 ON s2.ID = c.LINK2
    WHERE c.Name LIKE '%' || ? || '%' COLLATE NOCASE
    ORDER BY c.Name COLLATE NOCASE;
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(sql, (keyword,))
        rows = cur.fetchall()

    result = []
    for r in rows:
        link1_mh = r["Link1Manhole"]
        link2_mh = r["Link2Manhole"]
        same = "✅ Yes" if link1_mh and link2_mh and str(link1_mh).strip().lower() == str(link2_mh).strip().lower() else "❌ No"
        result.append((
            r["Link1Name"],
            link1_mh,
            r["CableName"],
            r["CableLength"],
            r["Link2Name"],
            link2_mh,
            same
        ))
    return result


class SortableFilterTable(ttk.Frame):
    def __init__(self, master, columns, rows):
        super().__init__(master)
        self.columns = columns
        self.all_rows = rows[:]
        self.filtered_rows = rows[:]
        self.sort_state = {col: None for col in self.columns}

        # Styles
        style = ttk.Style(self)
        if "clam" in style.theme_names():
            style.theme_use("clam")
        style.configure("Treeview", rowheight=24)
        style.configure("TEntry", padding=2)

        # Top bar with Export button
        topbar = ttk.Frame(self)
        topbar.grid(row=0, column=0, sticky="ew", padx=8, pady=(8, 4))
        topbar.columnconfigure(0, weight=1)

        export_btn = ttk.Button(topbar, text="Export CSV", command=self.export_csv)
        export_btn.pack(side="right")

        # Filters
        filter_frame = ttk.Frame(self)
        filter_frame.grid(row=1, column=0, sticky="ew", padx=8, pady=(4, 4))
        for i in range(len(columns)):
            filter_frame.columnconfigure(i, weight=1)

        self.filter_vars = []
        for i, col in enumerate(columns):
            col_frame = ttk.Frame(filter_frame)
            col_frame.grid(row=0, column=i, sticky="ew", padx=(0 if i == 0 else 6, 0))
            ttk.Label(col_frame, text=f"Filter {col}").pack(anchor="w")
            var = tk.StringVar()
            ent = ttk.Entry(col_frame, textvariable=var)
            ent.pack(fill="x")
            var.trace_add("write", self._on_filter_changed)
            self.filter_vars.append(var)

        # Treeview
        tree_frame = ttk.Frame(self)
        tree_frame.grid(row=2, column=0, sticky="nsew", padx=8, pady=4)
        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=self.columns,
            show="headings",
            selectmode="browse",
        )
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tree_frame.rowconfigure(0, weight=1)
        tree_frame.columnconfigure(0, weight=1)

        # Column setup (added width for Cable Length)
        widths = [180, 120, 280, 120, 180, 120, 120]
        anchors = ["center", "center", "center", "center", "center", "center", "center"]
        for i, col in enumerate(self.columns):
            self.tree.heading(col, text=col, command=lambda c=col: self._sort_by(c))
            self.tree.column(col, width=widths[i], anchor=anchors[i], stretch=True)

        # Status bar
        self.status_var = tk.StringVar()
        status = ttk.Label(self, textvariable=self.status_var, anchor="w")
        status.grid(row=3, column=0, sticky="ew", padx=8, pady=(4, 8))

        self._refresh_tree()

    def _on_filter_changed(self, *_):
        terms = [v.get().strip().lower() for v in self.filter_vars]

        def row_matches(row):
            for idx, term in enumerate(terms):
                if term and ("" if row[idx] is None else str(row[idx]).lower()).find(term) == -1:
                    return False
            return True

        self.filtered_rows = [r for r in self.all_rows if row_matches(r)]
        active_sort = next((c for c, s in self.sort_state.items() if s), None)
        if active_sort:
            self._apply_sort_to_filtered(active_sort, self.sort_state[active_sort])
        self._refresh_tree()

    def _sort_by(self, column):
        state = self.sort_state[column]
        new_state = "asc" if state is None else ("desc" if state == "asc" else None)
        for c in self.columns:
            self.sort_state[c] = None
        self.sort_state[column] = new_state
        if new_state:
            self._apply_sort_to_filtered(column, new_state)
        self._refresh_tree()

    def _apply_sort_to_filtered(self, column, direction):
        idx = self.columns.index(column)
        def keyfn(row):
            val = row[idx]
            # try numeric sort for Cable Length if possible
            if self.columns[idx] == "Cable Length":
                try:
                    return (False, float(val))
                except (TypeError, ValueError):
                    pass
            return (val is None, ("" if val is None else str(val)).lower())
        reverse = (direction == "desc")
        self.filtered_rows.sort(key=keyfn, reverse=reverse)

    def _refresh_tree(self):
        self.tree.delete(*self.tree.get_children())
        for r in self.filtered_rows:
            # pretty print length if it looks numeric; otherwise show as-is
            display = []
            for col, v in zip(self.columns, r):
                if col == "Cable Length" and v not in (None, ""):
                    try:
                        num = float(v)
                        # drop trailing .0 for integers
                        display.append(str(int(num)) if num.is_integer() else str(num))
                    except (TypeError, ValueError):
                        display.append("" if v is None else str(v))
                else:
                    display.append("" if v is None else str(v))
            self.tree.insert("", "end", values=tuple(display))
        self.status_var.set(f"Rows: {len(self.filtered_rows)} (total {len(self.all_rows)})")

    def export_csv(self):
        if not self.filtered_rows:
            messagebox.showinfo("Export CSV", "No rows to export.")
            return
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Save filtered results as CSV",
            initialfile="cable_extracts.csv",
        )
        if not filepath:
            return
        try:
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(self.columns)
                for row in self.filtered_rows:
                    writer.writerow(["" if v is None else v for v in row])
            messagebox.showinfo("Export CSV", f"Saved:\n{filepath}")
        except Exception as e:
            messagebox.showerror("Export CSV", f"Failed to save CSV:\n{e}")


def main():
    try:
        rows = fetch_rows(DB_FILENAME, KEYWORD)
    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Error", f"Failed to load data:\n{e}")
        return

    root = tk.Tk()
    root.title("FSS Cables — LINK1 / MANHOLE / Cable / Length / LINK2 / MANHOLE / Same Manhole")
    root.geometry("1280x600")

    columns = [
        "LINK1 Splice Case",
        "LINK1 Manhole",
        "Cable Name",
        "Cable Length",
        "LINK2 Splice Case",
        "LINK2 Manhole",
        "Same Manhole"
    ]
    table = SortableFilterTable(root, columns, rows)
    table.pack(fill="both", expand=True)

    root.mainloop()


if __name__ == "__main__":
    main()
