import time
from pathlib import Path
from storage.datalakes.datalake_tria import Datalake
from storage.datalakes.datalake_sql import DatalakeSQL
from storage.datamarts.datamart_sqlite import DatamartSQLite
from storage.datamarts.datamart_shelve import DatamartShelve
import matplotlib.pyplot as plt


class StorageBenchmark:
    def __init__(self):
        self.results = []
        self.items = 0

    def generate_test_data(self, n=50):
        data = [{
            "book_id": i,
            "raw_text": (
                f"Title: Book {i}\nAuthor: Author {i%5}\nLanguage: English\n"
                f"*** START OF THE PROJECT GUTENBERG EBOOK ***\n"
                f"Content {i}\n*** END OF THE PROJECT GUTENBERG EBOOK ***"
            ),
            "header": f"Title: Book {i}\nAuthor: Author {i%5}\nLanguage: English",
            "body": f"Content {i}"
        } for i in range(1, n + 1)]
        self.items = n
        return data

    def _time_it(self, name, func):
        start = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - start
        self.results.append((name, elapsed, result))
        return elapsed

    def run_datalake_benchmarks(self, test_data):
        print("Benchmarking Datalakes...")
        lake = Datalake(root="bench_datalake_files")
        self._time_it(
            "Datalake_File",
            lambda: [lake.save_raw(d["book_id"], d["raw_text"]) for d in test_data]
        )

        lake_sql = DatalakeSQL("bench_datalake_sql/books.db")
        try:
            conn = lake_sql.conn
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-20000;")
        except Exception:
            pass

        self._time_it(
            "Datalake_SQL",
            lambda: [lake_sql.save_raw(d["book_id"], d["header"], d["body"]) for d in test_data]
        )
        lake_sql.conn.close()

    def run_datamart_benchmarks(self, test_data):
        print("Benchmarking Datamarts...")

        metadata = [{
            "book_id": d["book_id"],
            "title": f"Book {d['book_id']}",
            "author": f"Author {d['book_id'] % 5}",
            "language": "English",
            "header_path": f"path/{d['book_id']}_h.txt",
            "body_path": f"path/{d['book_id']}_b.txt"
        } for d in test_data]

        dm_sql = DatamartSQLite("bench_datamart.sqlite")
        try:
            conn = dm_sql.conn
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-20000;")
        except Exception:
            pass

        self._time_it("Datamart_SQLite", lambda: dm_sql.upsert_many(metadata))
        dm_sql.close()
        dm_sh = DatamartShelve("bench_datamart_shelve.db")
        self._time_it("Datamart_Shelve", lambda: dm_sh.upsert_many(metadata))
        dm_sh.close()

    def print_results(self):
        print("\n" + "=" * 50)
        print("STORAGE BENCHMARK RESULTS")
        print("=" * 50)
        items = self.items or 1

        for name, time_taken, _ in self.results:
            avg_ms = (time_taken * 1000) / items
            throughput = items / time_taken if time_taken > 0 else 0
            print(f"{name:<18} | {time_taken:6.3f}s total | {avg_ms:6.2f} ms/item | {throughput:6.0f} items/s")

        datalakes = [r for r in self.results if "Datalake" in r[0]]
        datamarts = [r for r in self.results if "Datamart" in r[0]]

        if datalakes:
            fastest_lake = min(datalakes, key=lambda x: x[1])
            print(f"\nFastest Datalake: {fastest_lake[0]} ({fastest_lake[1]:.3f}s)")

        if datamarts:
            fastest_mart = min(datamarts, key=lambda x: x[1])
            print(f"Fastest Datamart: {fastest_mart[0]} ({fastest_mart[1]:.3f}s)")

    def plot_results(self, outfile="bench.png"):
        if not self.results:
            print("No results to plot.")
            return

        names = [r[0] for r in self.results]
        times = [r[1] for r in self.results]

        plt.figure(figsize=(9, 5))
        bars = plt.bar(names, times, color='steelblue')
        for bar, t in zip(bars, times):
            plt.text(
                bar.get_x() + bar.get_width() / 2, bar.get_height(),
                f"{t:.3f}s", ha='center', va='bottom', fontsize=9
            )

        plt.title("Storage Benchmark")
        plt.ylabel("Total time (s)")
        plt.xlabel("Storage Strategy")
        plt.ylim(0, max(times) * 1.25)
        plt.tight_layout()
        plt.savefig(outfile)
        print(f"[PLOT] Saved results chart to {outfile}")

    def cleanup(self):
        paths = [
            "bench_datalake_files",
            "bench_datalake_sql",
            "bench_datamart.sqlite",
            "bench_datamart_shelve.db"
        ]
        for path in paths:
            p = Path(path)
            if p.exists():
                if p.is_file():
                    p.unlink()
                else:
                    import shutil
                    shutil.rmtree(path)


def main():
    bench = StorageBenchmark()
    try:
        test_data = bench.generate_test_data(200)
        bench.run_datalake_benchmarks(test_data)
        bench.run_datamart_benchmarks(test_data)
        bench.print_results()
        bench.plot_results("bench.png")
    finally:
        bench.cleanup()


if __name__ == "__main__":
    main()
