const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const mongoose = require('mongoose');
const { Sequelize, DataTypes } = require('sequelize');
const kafka = require('kafka-node');
const path = require('path');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const multer = require('multer');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Ensure upload directory exists
const uploadDir = path.join(__dirname, 'public/uploads');
if (!fs.existsSync(uploadDir)){
    fs.mkdirSync(uploadDir, { recursive: true });
}

// --- Multer Configuration ---
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, 'public/uploads/');
    },
    filename: (req, file, cb) => {
        cb(null, Date.now() + '-' + file.originalname);
    }
});
const upload = multer({ storage: storage });

// --- Session Setup ---
app.use(session({
    secret: 'college_secret_key',
    resave: false,
    saveUninitialized: false
}));

// --- Middleware ---
const requireLogin = (req, res, next) => {
    if (!req.session.userId) return res.redirect('/login');
    next();
};

const requireAdmin = (req, res, next) => {
    if (req.session.role !== 'Admin') return res.status(403).send("Access Denied");
    next();
};

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

(async () => {
    // 1. MySQL Connection
    await wait(10000); 
    const sequelize = new Sequelize('college_db', 'root', 'rootpassword', {
        host: process.env.DB_HOST || 'mysql_db',
        dialect: 'mysql',
        logging: false
    });

    const User = sequelize.define('User', {
        username: { type: DataTypes.STRING, allowNull: false, unique: true },
        password: { type: DataTypes.STRING, allowNull: false },
        role: { type: DataTypes.ENUM('Admin', 'Student', 'Faculty', 'Staff', 'Alumni'), allowNull: false },
        isActive: { type: DataTypes.BOOLEAN, defaultValue: true }
    });

    // 2. MongoDB Connection
    mongoose.connect(process.env.MONGO_URL)
        .then(() => console.log("MongoDB Connected"))
        .catch(err => console.log(err));

    // -- Schemas --
    const ListingSchema = new mongoose.Schema({
        title: String,
        category: String,
        description: String,
        postedBy: String,
        price: Number,
        image: String
    });
    const Listing = mongoose.model('Listing', ListingSchema);

    const ChatSchema = new mongoose.Schema({
        room: String,
        sender: String,
        message: String,
        timestamp: { type: Date, default: Date.now }
    });
    const Chat = mongoose.model('Chat', ChatSchema);

    // 3. Kafka Producer
    const client = new kafka.KafkaClient({ kafkaHost: process.env.KAFKA_BROKER });
    const producer = new kafka.Producer(client);

    app.set('view engine', 'ejs');
    app.set('views', path.join(__dirname, 'views'));
    
    app.use(express.static('public')); // Serve uploads
    app.use(express.urlencoded({ extended: true }));

    app.use((req, res, next) => {
        res.locals.user = req.session.username || null;
        res.locals.role = req.session.role || null;
        next();
    });

    // --- ROUTES ---

    app.get('/', (req, res) => res.render('index'));

    app.get('/register', (req, res) => res.render('register'));
    app.post('/register', async (req, res) => {
        const { username, password, role } = req.body;
        try {
            const hashedPassword = await bcrypt.hash(password, 10);
            await User.create({ username, password: hashedPassword, role });
            res.redirect('/login');
        } catch (e) { res.send("Error: Username taken"); }
    });

    app.get('/login', (req, res) => res.render('login'));
    app.post('/login', async (req, res) => {
        const { username, password } = req.body;
        const user = await User.findOne({ where: { username } });
        if (user && await bcrypt.compare(password, user.password)) {
            if (!user.isActive) return res.send("Account Frozen");
            req.session.userId = user.id;
            req.session.username = user.username;
            req.session.role = user.role;
            return res.redirect('/dashboard');
        }
        res.send("Invalid credentials");
    });

    app.get('/logout', (req, res) => req.session.destroy(() => res.redirect('/')));

    app.get('/dashboard', requireLogin, async (req, res) => {
        const listings = await Listing.find();
        res.render('dashboard', { listings });
    });

    // Post Listing (With Image)
    app.post('/post-listing', requireLogin, upload.single('image'), async (req, res) => {
        const { title, category, description, price } = req.body;
        const imagePath = req.file ? '/uploads/' + req.file.filename : null;

        await Listing.create({ 
            title, category, description, postedBy: req.session.username, price,
            image: imagePath
        });

        const payloads = [{ topic: 'new_listings', messages: JSON.stringify({ category, user: req.session.username, price }) }];
        producer.send(payloads, (err, data) => {});
        res.redirect('/dashboard');
    });


   

    // Delete Listing Route
    app.post('/delete-listing/:id', requireLogin, async (req, res) => {
        try {
            const listing = await Listing.findById(req.params.id);
            // Security Check: Ensure the logged-in user is the owner
            if (listing && listing.postedBy === req.session.username) {
                await Listing.findByIdAndDelete(req.params.id);
            }
        } catch (err) {
            console.error(err);
        }
        res.redirect('/dashboard');
    });

   

    // Chat Room
    app.get('/chat', requireLogin, async (req, res) => {
        const { room, withUser } = req.query;
        if (!room.includes(req.session.username)) return res.send("Access Denied");
        res.render('chat', { room, withUser, currentUser: req.session.username });
    });

    // Inbox
    app.get('/inbox', requireLogin, async (req, res) => {
        const myUser = req.session.username;
        const chats = await Chat.find({ room: { $regex: myUser } }).distinct('room');
        const conversations = chats.map(room => {
            const parts = room.split('_'); 
            const otherPerson = parts[1] === myUser ? parts[2] : parts[1];
            return { room, otherPerson, listingId: parts[0] };
        });
        res.render('inbox', { conversations });
    });

    app.get('/admin/users', requireLogin, requireAdmin, async (req, res) => {
        const users = await User.findAll();
        res.render('admin', { users });
    });

    // Socket.io
    io.on('connection', (socket) => {
        socket.on('join_room', async ({ room }) => {
            socket.join(room);
            const history = await Chat.find({ room }).sort({ timestamp: 1 });
            socket.emit('load_history', history);
        });

        socket.on('chat_message', async ({ room, sender, message }) => {
            await Chat.create({ room, sender, message });
            io.to(room).emit('receive_message', { sender, message });
        });
    });

    await sequelize.sync({ alter: true });
    server.listen(3000, () => console.log('Node Server running on port 3000'));
})();