import tkinter as tk
from tkinter import filedialog, simpledialog
import fitz  # PyMuPDF
from PIL import Image, ImageTk

class PDFEditor:
    def __init__(self, root):
        self.root = root
        self.root.title("PDF Editor (Light)")

        self.doc = None
        self.page_index = 0
        self.text_items = []  # (page, x, y, text)

        # Canvas
        self.canvas = tk.Canvas(root, bg="gray")
        self.canvas.pack(fill=tk.BOTH, expand=True)

        # Buttons
        frame = tk.Frame(root)
        frame.pack()

        tk.Button(frame, text="PDF laden", command=self.load_pdf).pack(side=tk.LEFT)
        tk.Button(frame, text="Speichern", command=self.save_pdf).pack(side=tk.LEFT)
        tk.Button(frame, text="Seite +", command=self.next_page).pack(side=tk.LEFT)
        tk.Button(frame, text="Seite -", command=self.prev_page).pack(side=tk.LEFT)

        self.canvas.bind("<Button-1>", self.add_text)

        self.img = None
        self.tk_img = None

    def load_pdf(self):
        path = filedialog.askopenfilename(filetypes=[("PDF", "*.pdf")])
        if not path:
            return

        self.doc = fitz.open(path)
        self.page_index = 0
        self.render_page()

    def render_page(self):
        page = self.doc[self.page_index]
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))

        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        self.img = img
        self.tk_img = ImageTk.PhotoImage(img)

        self.canvas.delete("all")
        self.canvas.create_image(0, 0, anchor=tk.NW, image=self.tk_img)

        # redraw text overlays
        for p, x, y, text in self.text_items:
            if p == self.page_index:
                self.canvas.create_text(x, y, text=text, fill="red", font=("Arial", 14))

    def add_text(self, event):
        if not self.doc:
            return

        text = simpledialog.askstring("Text", "Text eingeben:")
        if not text:
            return

        self.text_items.append((self.page_index, event.x, event.y, text))
        self.render_page()

    def next_page(self):
        if self.doc and self.page_index < len(self.doc) - 1:
            self.page_index += 1
            self.render_page()

    def prev_page(self):
        if self.doc and self.page_index > 0:
            self.page_index -= 1
            self.render_page()

    def save_pdf(self):
        if not self.doc:
            return

        for p, x, y, text in self.text_items:
            page = self.doc[p]

            # Skalierung beachten (Bild ist 2x zoom)
            page.insert_text((x/2, y/2), text, fontsize=12, color=(1, 0, 0))

        save_path = filedialog.asksaveasfilename(defaultextension=".pdf")
        if save_path:
            self.doc.save(save_path)


if __name__ == "__main__":
    root = tk.Tk()
    app = PDFEditor(root)
    root.geometry("1000x700")
    root.mainloop()